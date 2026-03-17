// Manages the slide-in detail panel for Gantt block clicks.
// Each block type (PRODUCTION, CLEANING, HOLD, BLOCKED, etc.) renders a
// specialised detail view with all available data from the planning run.

var GanttDetail = (function () {

  var _planData = {};

  function init(planResponse) {
    _planData = planResponse || {};
  }

  // ── Panel open / close ──────────────────────────────────────────

  function openPanel(html) {
    var panel = document.getElementById('gantt-detail-panel');
    var overlay = document.getElementById('gantt-detail-overlay');
    if (!panel || !overlay) return;
    panel.innerHTML = html;
    panel.classList.add('open');
    overlay.classList.add('active');
  }

  function closePanel() {
    var panel = document.getElementById('gantt-detail-panel');
    var overlay = document.getElementById('gantt-detail-overlay');
    if (panel) panel.classList.remove('open');
    if (overlay) overlay.classList.remove('active');
  }

  // ── Click dispatcher ────────────────────────────────────────────

  function onBlockClick(block) {
    if (!block) return;
    var bt = (block.block_type || '').toUpperCase();
    switch (bt) {
      case 'PRODUCTION':  return openPanel(renderProductionDetail(block));
      case 'CLEANING':    return openPanel(renderCleaningDetail(block));
      case 'HOLD':        return openPanel(renderHoldDetail(block));
      case 'BLOCKED':     return openPanel(renderBlockedDetail(block));
      case 'SETUP':       return openPanel(renderSetupDetail(block));
      case 'MAINTENANCE': return openPanel(renderMaintenanceDetail(block));
      case 'PRE_COOL':    return openPanel(renderPreCoolDetail(block));
      default:            return openPanel(renderGenericDetail(block));
    }
  }

  // ── Data helpers ────────────────────────────────────────────────

  function getSalesOrder(orderId) {
    return (_planData.sales_orders || []).find(function (so) { return so.order_id === orderId; }) || {};
  }

  function getSalesOrderFromPO(poId) {
    if (!poId) return {};
    var parts = poId.split('-');
    if (parts.length >= 4 && parts[0] === 'PO') {
      var orderId = parts.slice(1, -1).join('-');
      return getSalesOrder(orderId);
    }
    return {};
  }

  function getRecipe(productId) {
    if (!productId) return {};
    var recipes = _planData.recipes || [];
    if (Array.isArray(recipes)) {
      return recipes.find(function (r) { return r.product_id === productId; }) || {};
    }
    return recipes[productId] || {};
  }

  function getRecipeSteps(productId) {
    var recipe = getRecipe(productId);
    return recipe.steps || [];
  }

  function getQASteps(productId) {
    return getRecipeSteps(productId).filter(function (s) { return s.qa_check_required == 1; });
  }

  function getBOM(productId) {
    return (_planData.recipe_bom || []).filter(function (b) { return b.product_id === productId; });
  }

  function getCIPProcedure(cleanType) {
    return (_planData.cip_procedures || [])
      .filter(function (c) { return c.cip_type === cleanType; })
      .sort(function (a, b) { return (a.step_number || 0) - (b.step_number || 0); });
  }

  function getMachineInfo(machineId) {
    return (_planData.machines || []).find(function (m) { return m.machine_id === machineId; }) || {};
  }

  function getInventoryForIngredient(ingredientId) {
    return (_planData.inventory || []).find(function (i) { return i.item_id === ingredientId || i.ingredient_id === ingredientId; });
  }

  function stockBadge(ingredientId, requiredQty) {
    var inv = getInventoryForIngredient(ingredientId);
    if (!inv) return '<span class="gd-badge gd-badge-grey">Unknown</span>';
    var stock = inv.qty_lbs || inv.stock_on_hand_lbs || 0;
    if (stock >= requiredQty) return '<span class="gd-badge gd-badge-green">\u2705 ' + stock.toLocaleString() + ' lbs</span>';
    if (stock > 0) return '<span class="gd-badge gd-badge-amber">\u26a0\ufe0f ' + stock.toLocaleString() + ' lbs</span>';
    return '<span class="gd-badge gd-badge-red">\ud83d\udeab 0 lbs</span>';
  }

  function priorityBadge(priority) {
    if (!priority) return '';
    var colors = { CRITICAL: 'gd-badge-red', HIGH: 'gd-badge-orange', MEDIUM: 'gd-badge-amber', LOW: 'gd-badge-blue' };
    return '<span class="gd-badge ' + (colors[priority] || 'gd-badge-grey') + '">' + priority + '</span>';
  }

  function fmtDt(dt) {
    if (!dt) return '\u2014';
    return String(dt).replace('T', ' ').substring(0, 16);
  }

  function allergenTags(allergens) {
    if (!allergens || allergens.length === 0) return '<span class="gd-badge gd-badge-green">NONE</span>';
    return allergens.map(function (a) { return '<span class="gd-badge gd-badge-red">' + a + '</span>'; }).join(' ');
  }

  function addMinutes(dtStr, minutes) {
    if (!dtStr || !minutes) return dtStr;
    try {
      var d = new Date(dtStr);
      d.setMinutes(d.getMinutes() + parseInt(minutes, 10));
      return d.toISOString().replace('T', ' ').substring(0, 16);
    } catch (e) {
      return dtStr;
    }
  }

  function esc(str) {
    if (!str) return '';
    var d = document.createElement('div');
    d.textContent = String(str);
    return d.innerHTML;
  }

  // ── Renderers ───────────────────────────────────────────────────

  function renderProductionDetail(block) {
    var soItem   = getSalesOrderFromPO(block.process_order_id);
    var machine  = getMachineInfo(block.machine_id);
    var steps    = getRecipeSteps(block.product_id);
    var qaSteps  = getQASteps(block.product_id);
    var bom      = getBOM(block.product_id);
    var thisStep = steps.find(function (s) { return s.step_number == block.step_number; }) || {};

    var qaRows = qaSteps.map(function (s) {
      var spec = s.spec_unit === 'PASS/FAIL'
        ? 'Pass / Fail'
        : (s.spec_min != null ? s.spec_min : '\u2014') + ' \u2013 ' + (s.spec_max != null ? s.spec_max : '\u2014') + ' ' + (s.spec_unit || '');
      var isCcp = s.critical_param && s.critical_param.indexOf('CCP') >= 0;
      return '<tr><td>Step ' + s.step_number + '</td><td>' + esc(s.step_name) + '</td><td>' + spec +
        '</td><td>' + esc(s.critical_param || '\u2014') + '</td><td class="' + (isCcp ? 'gd-text-red' : '') +
        '">' + (isCcp ? '\u26a0\ufe0f CCP' : '\u2713') + '</td></tr>';
    }).join('');

    var bomRows = bom.length > 0
      ? bom.map(function (b) {
          return '<tr><td>' + esc(b.ingredient_name) + '</td><td>' + b.qty_per_batch_lbs + ' lbs</td><td>' +
            stockBadge(b.ingredient_id, b.qty_per_batch_lbs) + '</td></tr>';
        }).join('')
      : '<tr><td colspan="3" class="gd-text-muted">BOM data not loaded</td></tr>';

    var stepsTimeline = steps.map(function (s) {
      var activeClass = (s.step_number == block.step_number) ? ' gd-step-active' : '';
      var stepTypeLower = (s.step_type || 'processing').toLowerCase();
      return '<div class="gd-step-row' + activeClass + '">' +
        '<span class="gd-step-num">' + s.step_number + '</span>' +
        '<span class="gd-step-type-badge gd-step-' + stepTypeLower + '">' + (s.step_type || '') + '</span>' +
        '<span class="gd-step-name">' + esc(s.step_name) + '</span>' +
        '<span class="gd-step-dur">' + s.duration_min + 'min</span>' +
        (s.qa_check_required ? '<span class="gd-step-qa">QA</span>' : '') +
        '</div>';
    }).join('');

    return '<div class="gd-inner">' +
      '<div class="gd-header">' +
        '<div class="gd-title"><h2>' + esc(block.product_name) + '</h2>' +
          '<div class="gd-subtitle">Step ' + (block.step_number || '?') + ': ' + esc(block.step_name || '') +
          ' &nbsp;' + priorityBadge(block.priority) +
          (block.is_ccp ? ' <span class="gd-badge gd-badge-red">\u26a0\ufe0f CCP</span>' : '') +
        '</div></div>' +
        '<button class="gd-close" onclick="GanttDetail.closePanel()">\u2715</button>' +
      '</div>' +

      '<div class="gd-section"><h3>\ud83d\udce6 Sales Order</h3>' +
        '<div class="gd-grid">' +
          kvHtml('Order ID', soItem.order_id || block.process_order_id || '\u2014') +
          kvHtml('Customer', soItem.customer_name || block.customer_name || '\u2014') +
          kvHtml('Required Ship', fmtDt(soItem.required_ship_date || block.required_by)) +
          kvHtml('Priority', priorityBadge(block.priority)) +
          kvHtml('Batch', block.batch_info || '\u2014') +
          kvHtml('Qty Ordered', soItem.qty_ordered_lbs ? Number(soItem.qty_ordered_lbs).toLocaleString() + ' lbs' : '\u2014') +
      '</div></div>' +

      '<div class="gd-section"><h3>\ud83c\udfed Production Details</h3>' +
        '<div class="gd-grid">' +
          kvHtml('Process Order', block.process_order_id || '\u2014') +
          kvHtml('Machine', machine.machine_name || block.machine_name || block.machine_id || '\u2014') +
          kvHtml('Plant / Line', (machine.plant_id || '\u2014') + ' / ' + (machine.line_id || '\u2014')) +
          kvHtml('Start', fmtDt(block.start_datetime || block.start)) +
          kvHtml('End', fmtDt(block.end_datetime || block.end)) +
          kvHtml('Duration', (block.duration_min || 0) + ' min') +
      '</div></div>' +

      '<div class="gd-section"><h3>\ud83d\udccb Recipe Step Detail \u2014 Step ' + (block.step_number || '?') + '</h3>' +
        '<div class="gd-grid">' +
          kvHtml('Step Type', thisStep.step_type || '\u2014') +
          kvHtml('Temp Range', (thisStep.temp_f_min != null ? thisStep.temp_f_min + '\u00b0F' : '\u2014') + ' \u2013 ' +
            (thisStep.temp_f_max != null ? thisStep.temp_f_max + '\u00b0F' : '\u2014')) +
          kvHtml('Wait After', (thisStep.wait_after_min || 0) + ' min') +
          (block.is_ccp ? kvHtml('\u26a0\ufe0f CCP \u2014 Critical Param', thisStep.critical_param || '\u2014', 'gd-kv-ccp') +
            kvHtml('Spec Range', (thisStep.spec_unit === 'PASS/FAIL' ? 'Pass / Fail' :
              (thisStep.spec_min != null ? thisStep.spec_min : '\u2014') + ' \u2013 ' +
              (thisStep.spec_max != null ? thisStep.spec_max : '\u2014') + ' ' + (thisStep.spec_unit || '')), 'gd-kv-ccp') : '') +
        '</div>' +
        (thisStep.operator_notes ? '<div class="gd-notes"><label>Operator Notes</label><p>' + esc(thisStep.operator_notes) + '</p></div>' : '') +
      '</div>' +

      '<div class="gd-section"><h3>\u26a0\ufe0f Allergens</h3>' +
        '<div class="gd-allergen-row"><label>This product contains:</label><div>' + allergenTags(block.allergens) + '</div></div>' +
        (block.notes ? '<p class="gd-detail-note">' + esc(block.notes) + '</p>' : '') +
      '</div>' +

      '<div class="gd-section"><h3>\ud83e\uddea Recipe Bill of Materials</h3>' +
        '<table class="gd-table"><thead><tr><th>Ingredient</th><th>Qty / Batch</th><th>Stock</th></tr></thead>' +
        '<tbody>' + bomRows + '</tbody></table></div>' +

      '<div class="gd-section"><h3>\u2705 QA Checkpoints</h3>' +
        (qaRows ? '<table class="gd-table"><thead><tr><th>Step</th><th>Name</th><th>Spec</th><th>Parameter</th><th>Type</th></tr></thead><tbody>' + qaRows + '</tbody></table>'
                : '<p class="gd-text-muted">No QA steps loaded</p>') +
      '</div>' +

      (stepsTimeline ? '<div class="gd-section"><h3>\ud83d\udd22 All Recipe Steps</h3><div class="gd-steps-timeline">' + stepsTimeline + '</div></div>' : '') +
    '</div>';
  }


  function renderCleaningDetail(block) {
    var machine = getMachineInfo(block.machine_id);
    var cipSteps = getCIPProcedure(block.clean_type);
    var totalCipMin = cipSteps.reduce(function (s, c) { return s + (c.duration_min || 0); }, 0);

    var fromAllergens = block.from_allergens || [];
    var toAllergens = block.to_allergens || [];
    var removedAllergens = fromAllergens.filter(function (a) { return toAllergens.indexOf(a) === -1; });
    var addedAllergens = toAllergens.filter(function (a) { return fromAllergens.indexOf(a) === -1; });

    var cleanTypeLabels = {
      'BASIC_CLEAN': '\ud83d\udfe1 Basic Clean',
      'ALLERGEN_CIP': '\ud83d\udfe0 Allergen CIP',
      'DEEP_CLEAN': '\ud83d\udd34 Deep Clean',
      'SANITATION_ONLY': '\ud83d\udfe2 Sanitisation Only'
    };

    var cipRows = cipSteps.map(function (s) {
      return '<div class="gd-cip-step">' +
        '<span class="gd-cip-step-num">' + s.step_number + '</span>' +
        '<div class="gd-cip-step-body"><strong>' + esc(s.step_name) + '</strong>' +
          '<span class="gd-cip-meta">' +
            (s.chemical_product && s.chemical_product !== 'N/A' ? esc(s.chemical_product) : '') +
            (s.concentration_pct ? ' @ ' + s.concentration_pct : '') +
            (s.water_temp_f ? ' \u2022 ' + s.water_temp_f + '\u00b0F' : '') +
            ' \u2022 ' + s.duration_min + ' min' +
            (s.atp_test ? ' <span class="gd-badge gd-badge-amber">ATP TEST</span>' : '') +
          '</span>' +
          (s.notes ? '<p class="gd-cip-note">' + esc(s.notes) + '</p>' : '') +
        '</div></div>';
    }).join('');

    return '<div class="gd-inner">' +
      '<div class="gd-header gd-header-cleaning">' +
        '<div class="gd-title"><h2>\ud83e\uddf9 ' + (cleanTypeLabels[block.clean_type] || block.clean_type || 'Cleaning') + '</h2>' +
          '<div class="gd-subtitle">' + esc(machine.machine_name || block.machine_name || block.machine_id) + ' \u2022 ' + (block.duration_min || 0) + ' min</div>' +
        '</div>' +
        '<button class="gd-close" onclick="GanttDetail.closePanel()">\u2715</button>' +
      '</div>' +

      '<div class="gd-section"><h3>WHY THIS CLEANING WAS INSERTED</h3>' +
        '<div class="gd-allergen-switch">' +
          '<div class="gd-allergen-from"><label>Previous product</label><strong>' + esc(block.from_product_name || block.from_product || '\u2014') + '</strong>' +
            '<div class="gd-allergen-tags">' + allergenTags(fromAllergens) + '</div></div>' +
          '<div class="gd-allergen-arrow">\u2193</div>' +
          '<div class="gd-allergen-to"><label>Next product</label><strong>' + esc(block.to_product_name || block.to_product || '\u2014') + '</strong>' +
            '<div class="gd-allergen-tags">' + allergenTags(toAllergens) + '</div></div>' +
        '</div>' +
        (removedAllergens.length > 0
          ? '<div class="gd-allergen-removed"><label>\u274c Allergens being REMOVED:</label><div>' +
              removedAllergens.map(function (a) { return '<span class="gd-badge gd-badge-red">' + a + '</span>'; }).join(' ') +
            '</div><p class="gd-detail-note">FDA Big-9 requirement: these allergens must be eliminated before running a product that does not declare them.</p></div>'
          : '') +
        (addedAllergens.length > 0
          ? '<div class="gd-allergen-added"><label>\u2795 Allergens being ADDED:</label><div>' +
              addedAllergens.map(function (a) { return '<span class="gd-badge gd-badge-amber">' + a + '</span>'; }).join(' ') +
            '</div></div>'
          : '') +
      '</div>' +

      '<div class="gd-section"><h3>\u23f1\ufe0f Timing</h3>' +
        '<div class="gd-grid">' +
          kvHtml('Clean Start', fmtDt(block.start_datetime || block.start)) +
          kvHtml('Clean End', fmtDt(block.end_datetime || block.end)) +
          kvHtml('Clean Duration', (block.duration_min || 0) + ' min') +
          (block.atp_swab_required
            ? kvHtml('+ ATP Swab Hold', (block.hold_min || 45) + ' min') +
              kvHtml('Earliest Restart', addMinutes(block.end_datetime || block.end, block.hold_min || 45), 'gd-text-amber')
            : '') +
      '</div></div>' +

      (block.atp_swab_required
        ? '<div class="gd-section gd-atp-section"><h3>\ud83d\udd2c ATP Swab Required</h3>' +
            '<div class="gd-grid">' +
              kvHtml('Pass Threshold', '< ' + (block.atp_threshold_rlu || 100) + ' RLU (each point)') +
              kvHtml('Minimum Swab Points', '3') +
              kvHtml('Result Wait', (block.hold_min || 45) + ' min') +
            '</div>' +
            '<p class="gd-detail-warn">\u26a0\ufe0f Production CANNOT restart until QC confirms all swab points pass.</p>' +
          '</div>'
        : '') +

      '<div class="gd-section"><h3>\ud83d\udccb Cleaning Procedure: ' + esc(block.clean_type || '') + '</h3>' +
        (cipRows
          ? '<div class="gd-cip-steps-list">' + cipRows + '</div>' +
            '<div class="gd-cip-total">Total procedure time: <strong>' + totalCipMin + ' min</strong></div>'
          : '<p class="gd-text-muted">CIP procedure details not loaded</p>') +
      '</div>' +
    '</div>';
  }


  function renderHoldDetail(block) {
    return '<div class="gd-inner">' +
      '<div class="gd-header gd-header-hold">' +
        '<div class="gd-title"><h2>\u23f8 ATP Swab Hold</h2>' +
          '<div class="gd-subtitle">' + (block.duration_min || 0) + ' min</div></div>' +
        '<button class="gd-close" onclick="GanttDetail.closePanel()">\u2715</button>' +
      '</div>' +
      '<div class="gd-section"><h3>WHY THIS HOLD EXISTS</h3>' +
        '<p>' + esc(block.notes || block.hold_reason || 'Allergen CIP completed. Awaiting ATP swab laboratory result.') + '</p>' +
        '<div class="gd-grid" style="margin-top:12px">' +
          kvHtml('Hold Start', fmtDt(block.start_datetime || block.start)) +
          kvHtml('Hold End', fmtDt(block.end_datetime || block.end)) +
          kvHtml('Duration', (block.duration_min || 0) + ' min') +
      '</div></div>' +
      '<div class="gd-section"><h3>Required Before Restart</h3>' +
        '<ul class="gd-checklist">' +
          '<li>\u2713 All ATP swab points &lt; 100 RLU</li>' +
          '<li>\u2713 Visual inspection by QC complete</li>' +
          '<li>\u2713 Allergen clean log signed off</li>' +
          '<li>\u2713 QC Manager approval</li>' +
        '</ul>' +
      '</div>' +
    '</div>';
  }


  function renderBlockedDetail(block) {
    return '<div class="gd-inner">' +
      '<div class="gd-header gd-header-blocked">' +
        '<div class="gd-title"><h2>\ud83d\udeab Production Blocked</h2>' +
          '<div class="gd-subtitle">' + esc(block.product_name || block.product || '') + '</div></div>' +
        '<button class="gd-close" onclick="GanttDetail.closePanel()">\u2715</button>' +
      '</div>' +
      '<div class="gd-section"><h3>Block Reason</h3>' +
        '<div class="gd-grid">' +
          kvHtml('Process Order', block.process_order_id || '\u2014') +
          kvHtml('Blocked Ingredient', block.ingredient_blocked || '\u2014') +
          kvHtml('Reason', block.blocked_reason || '\u2014') +
          kvHtml('Earliest Unblock', block.unblock_date || 'Unknown \u2014 place PO', 'gd-text-amber') +
        '</div>' +
        (block.notes ? '<p class="gd-detail-warn">' + esc(block.notes) + '</p>' : '') +
      '</div>' +
    '</div>';
  }


  function renderSetupDetail(block) {
    var machine = getMachineInfo(block.machine_id);
    return '<div class="gd-inner">' +
      '<div class="gd-header gd-header-setup">' +
        '<div class="gd-title"><h2>\u2699\ufe0f Machine Setup</h2>' +
          '<div class="gd-subtitle">' + esc(block.product_name || block.product || '') + '</div></div>' +
        '<button class="gd-close" onclick="GanttDetail.closePanel()">\u2715</button>' +
      '</div>' +
      '<div class="gd-section">' +
        '<div class="gd-grid">' +
          kvHtml('Machine', machine.machine_name || block.machine_name || block.machine_id || '\u2014') +
          kvHtml('Setup Start', fmtDt(block.start_datetime || block.start)) +
          kvHtml('Setup End', fmtDt(block.end_datetime || block.end)) +
          kvHtml('Duration', (block.duration_min || 0) + ' min') +
        '</div>' +
        '<p style="margin-top:10px">' + esc(block.notes || 'Pre-production setup: verify machine settings, calibrations, and pre-start checks.') + '</p>' +
      '</div>' +
    '</div>';
  }


  function renderMaintenanceDetail(block) {
    return '<div class="gd-inner">' +
      '<div class="gd-header gd-header-maintenance">' +
        '<div class="gd-title"><h2>\ud83d\udd27 Maintenance Window</h2></div>' +
        '<button class="gd-close" onclick="GanttDetail.closePanel()">\u2715</button>' +
      '</div>' +
      '<div class="gd-section">' +
        '<div class="gd-grid">' +
          kvHtml('Start', fmtDt(block.start_datetime || block.start)) +
          kvHtml('End', fmtDt(block.end_datetime || block.end)) +
          kvHtml('Duration', (block.duration_min || 0) + ' min') +
        '</div>' +
        '<p style="margin-top:10px">' + esc(block.notes || 'Scheduled maintenance \u2014 no production during this window.') + '</p>' +
      '</div>' +
    '</div>';
  }


  function renderPreCoolDetail(block) {
    return '<div class="gd-inner">' +
      '<div class="gd-header gd-header-precool">' +
        '<div class="gd-title"><h2>\u2744\ufe0f IQF Pre-Cool</h2>' +
          '<div class="gd-subtitle">90 min required before first frozen product</div></div>' +
        '<button class="gd-close" onclick="GanttDetail.closePanel()">\u2715</button>' +
      '</div>' +
      '<div class="gd-section">' +
        '<div class="gd-grid">' +
          kvHtml('Start', fmtDt(block.start_datetime || block.start)) +
          kvHtml('End', fmtDt(block.end_datetime || block.end)) +
        '</div>' +
        '<p style="margin-top:10px">' + esc(block.notes || 'IQF tunnel must reach -38\u00b0F to -40\u00b0F before first product enters. Mandatory 90-min pre-cool.') + '</p>' +
      '</div>' +
    '</div>';
  }


  function renderGenericDetail(block) {
    var safeJson;
    try { safeJson = JSON.stringify(block, null, 2); } catch (e) { safeJson = '{}'; }
    return '<div class="gd-inner">' +
      '<div class="gd-header">' +
        '<div class="gd-title"><h2>' + esc(block.block_type || 'Block') + '</h2>' +
          '<div class="gd-subtitle">' + esc(block.product_name || block.product || '') + '</div></div>' +
        '<button class="gd-close" onclick="GanttDetail.closePanel()">\u2715</button>' +
      '</div>' +
      '<div class="gd-section"><pre style="white-space:pre-wrap;font-size:12px;color:#cbd5e1">' + esc(safeJson) + '</pre></div>' +
    '</div>';
  }


  // ── Micro-template helper ───────────────────────────────────────

  function kvHtml(label, value, extraClass) {
    return '<div class="gd-kv' + (extraClass ? ' ' + extraClass : '') + '">' +
      '<label>' + esc(label) + '</label><span class="gd-val">' + (value || '') + '</span></div>';
  }


  return { init: init, onBlockClick: onBlockClick, closePanel: closePanel };
})();
