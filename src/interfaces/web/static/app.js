(function () {
  var API_BASE = '';

  var chatMessages = document.getElementById('chat-messages');
  var chatInput = document.getElementById('chat-input');
  var chatSend = document.getElementById('chat-send');
  var chatDatasourceEl = document.getElementById('chat-datasource');
  var chatDatasourceWrap = document.getElementById('chat-datasource-wrap');
  var chatNoDsMsg = document.getElementById('chat-no-datasource-msg');
  var btnPlan = document.getElementById('btn-plan');
  var ganttHierarchyEl = document.getElementById('gantt-hierarchy');
  var salesOrdersEl = document.getElementById('sales-orders');
  var inventorySummaryEl = document.getElementById('inventory-summary');
  var materialShortagesEl = document.getElementById('material-shortages');
  var machineSchedulesEl = document.getElementById('machine-schedules');
  var validationIssuesEl = document.getElementById('validation-issues');
  var allergenWarningsEl = document.getElementById('allergen-warnings');
  var schedulingExceptionsEl = document.getElementById('scheduling-exceptions');
  var riskBannerEl = document.getElementById('risk-banner');
  var riskTextEl = document.getElementById('risk-text');
  var summaryPanelEl = document.getElementById('summary-panel');
  var allergenWarningsPanelEl = document.getElementById('allergen-warnings-panel');
  var exceptionsPanelEl = document.getElementById('exceptions-panel');

  var lastSelectedDatasourceIds = null;

  var BLOCK_COLORS = {
    PRODUCTION:  '#22c55e',
    SETUP:       '#3b82f6',
    CLEANING:    '#f97316',
    HOLD:        '#ef4444',
    PRE_COOL:    '#06b6d4',
    MAINTENANCE: '#6b7280',
    BLOCKED:     '#991b1b',
    EXCEPTION:   '#fbbf24'
  };

  var BLOCK_CLASS = {
    PRODUCTION:  'item-production',
    SETUP:       'item-setup',
    CLEANING:    'item-cleaning',
    HOLD:        'item-hold',
    PRE_COOL:    'item-precool',
    MAINTENANCE: 'item-maintenance',
    BLOCKED:     'item-blocked',
    EXCEPTION:   'item-exception'
  };

  setChatEnabled(false);

  // Planning detail tabs
  document.querySelectorAll('.planning-tab').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var ptab = this.getAttribute('data-ptab');
      document.querySelectorAll('.planning-tab').forEach(function (b) { b.classList.remove('active'); });
      document.querySelectorAll('.planning-panel').forEach(function (p) { p.classList.remove('active'); });
      this.classList.add('active');
      var panel = document.getElementById('planning-panel-' + ptab);
      if (panel) panel.classList.add('active');
    });
  });

  function appendMessage(content, role) {
    var div = document.createElement('div');
    div.className = 'msg ' + (role === 'user' ? 'user' : 'bot');
    div.textContent = content;
    chatMessages.appendChild(div);
    chatMessages.scrollTop = chatMessages.scrollHeight;
  }

  function getSelectedDatasourceIds() {
    if (!chatDatasourceEl) return null;
    var opts = chatDatasourceEl.selectedOptions;
    if (!opts || opts.length === 0) return null;
    var ids = [];
    for (var i = 0; i < opts.length; i++) {
      var v = opts[i].value;
      if (v && v !== '') ids.push(parseInt(v, 10));
    }
    return ids.length ? ids : null;
  }

  function saveDatasourceSelection() {
    lastSelectedDatasourceIds = getSelectedDatasourceIds();
  }

  function setChatEnabled(enabled) {
    if (chatInput) chatInput.disabled = !enabled;
    if (chatSend) chatSend.disabled = !enabled;
    if (btnPlan) btnPlan.disabled = !enabled;
  }

  async function postChat(message) {
    var body = { message: message };
    var ids = getSelectedDatasourceIds();
    if (ids && ids.length) body.datasource_ids = ids;
    var res = await fetch(API_BASE + '/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    if (!res.ok) throw new Error(res.statusText);
    return res.json();
  }

  function allergenBadgeHtml(allergen) {
    var a = (allergen || '').toUpperCase();
    var cls = 'allergen-badge';
    if (a.indexOf('WHEAT') >= 0 || a.indexOf('GLUTEN') >= 0) cls += ' wheat';
    else if (a.indexOf('DAIRY') >= 0 || a.indexOf('MILK') >= 0) cls += ' dairy';
    else if (a.indexOf('NUT') >= 0 || a.indexOf('TREE') >= 0) cls += ' nuts';
    else if (a.indexOf('SOY') >= 0) cls += ' soy';
    else if (a.indexOf('EGG') >= 0) cls += ' eggs';
    else if (a.indexOf('PEANUT') >= 0) cls += ' peanuts';
    else if (a.indexOf('SESAME') >= 0) cls += ' sesame';
    return '<span class="' + cls + '">' + allergen + '</span>';
  }

  function normalizeTask(t, i) {
    var start = t.start;
    var end = t.end;
    if (typeof start !== 'string') start = (start && start.toISOString) ? start.toISOString().slice(0, 10) : '2026-01-01';
    if (typeof end !== 'string') end = (end && end.toISOString) ? end.toISOString().slice(0, 10) : '2026-01-02';
    var machine = t.machine || t.machine_id || '';
    var machineName = t.machine_name || machine;
    var rawName = String(t.name || 'Task').replace(/^\[.*?\]\s*/, '');
    var displayName = machineName ? (machineName + ' \u2192 ' + rawName) : rawName;
    var blockType = (t.block_type || t.type || 'production').toUpperCase();
    var customClass = t.custom_class || 'bar-production';
    if (t.risk_level === 'high') customClass = 'bar-at-risk';

    var startStr = String(start).length >= 10 ? String(start).slice(0, 10) : start;
    var endStr = String(end).length >= 10 ? String(end).slice(0, 10) : end;
    if (startStr === endStr && t.duration_min && t.duration_min > 0) {
      endStr = startStr;
    }

    return {
      id: String(t.id || 'task-' + i),
      name: displayName,
      start: startStr,
      end: endStr,
      progress: typeof t.progress === 'number' ? t.progress : 0,
      custom_class: customClass,
      type: t.type || blockType.toLowerCase(),
      block_type: blockType,
      plant: t.plant || t.plant_id || '',
      line: t.line || t.line_id || '',
      machine: machine,
      machine_name: machineName,
      product: t.product || t.product_name || '',
      product_id: t.product_id || '',
      quantity: t.quantity,
      allergens: t.allergens || [],
      risk_level: t.risk_level || 'low',
      delivery_target: t.delivery_target,
      duration_min: t.duration_min || 0,
      notes: t.notes || '',
      is_ccp: t.is_ccp || false,
      atp_required: t.atp_required || false,
      clean_type: t.clean_type || '',
      process_order_id: t.process_order_id || '',
      batch_info: t.batch_info || '',
      priority: t.priority || '',
      color: t.color || BLOCK_COLORS[blockType] || '#6b7280',
      // Extra fields for detail panel
      step_number: t.step_number,
      step_name: t.step_name || '',
      product_name: t.product_name || t.product || '',
      start_datetime: t.start_datetime || t.start || '',
      end_datetime: t.end_datetime || t.end || '',
      from_product: t.from_product || '',
      from_product_name: t.from_product_name || '',
      from_allergens: t.from_allergens || [],
      to_product: t.to_product || '',
      to_product_name: t.to_product_name || '',
      to_allergens: t.to_allergens || [],
      atp_swab_required: t.atp_swab_required || t.atp_required || false,
      hold_min: t.hold_min,
      atp_threshold_rlu: t.atp_threshold_rlu,
      hold_reason: t.hold_reason || '',
      blocked_reason: t.blocked_reason || '',
      ingredient_blocked: t.ingredient_blocked || '',
      unblock_date: t.unblock_date || '',
      customer_name: t.customer_name || '',
      required_by: t.required_by || ''
    };
  }

  function popupHtml(task) {
    var bt = task.block_type || 'PRODUCTION';
    var parts = ['<div class="details-container">'];
    parts.push('<h5>' + (task.name || '') + '</h5>');

    if (task.risk_level === 'high') {
      parts.push('<p style="color:#c62828;font-weight:600;">At Risk</p>');
    } else if (task.risk_level === 'medium') {
      parts.push('<p style="color:#e65100;font-weight:600;">Tight Timeline</p>');
    }

    parts.push('<p><strong>Type:</strong> ' + bt + '</p>');

    if (task.process_order_id) parts.push('<p><strong>PO:</strong> ' + task.process_order_id + '</p>');
    if (task.product) parts.push('<p><strong>Product:</strong> ' + task.product + '</p>');
    if (task.priority) parts.push('<p><strong>Priority:</strong> ' + task.priority + '</p>');
    if (task.batch_info) parts.push('<p><strong>Batch:</strong> ' + task.batch_info + '</p>');
    if (task.machine_name) parts.push('<p><strong>Machine:</strong> ' + task.machine_name + '</p>');
    if (task.plant) parts.push('<p><strong>Plant:</strong> ' + task.plant + '</p>');
    if (task.line) parts.push('<p><strong>Line:</strong> ' + task.line + '</p>');
    if (task.duration_min) parts.push('<p><strong>Duration:</strong> ' + task.duration_min + ' min</p>');

    if (task.allergens && task.allergens.length > 0) {
      parts.push('<p><strong>Allergens:</strong> ' + task.allergens.map(allergenBadgeHtml).join('') + '</p>');
    }

    if (task.is_ccp) parts.push('<p style="color:#c62828;font-weight:600;">CCP - Quality Check Required</p>');
    if (task.atp_required) parts.push('<p style="color:#ef4444;font-weight:600;">ATP Swab Required</p>');
    if (task.clean_type) parts.push('<p><strong>Clean Type:</strong> ' + task.clean_type + '</p>');

    parts.push('<p><strong>Start:</strong> ' + task.start + '</p>');
    parts.push('<p><strong>End:</strong> ' + task.end + '</p>');

    if (task.delivery_target) parts.push('<p><strong>Delivery Target:</strong> ' + task.delivery_target + '</p>');
    if (task.notes) parts.push('<p><strong>Notes:</strong> ' + task.notes + '</p>');

    parts.push('</div>');
    return parts.join('');
  }

  function getTimelineClassName(task) {
    var bt = (task.block_type || '').toUpperCase();
    return BLOCK_CLASS[bt] || 'item-production';
  }

  function buildHierarchy(tasks) {
    var plants = {};
    for (var i = 0; i < tasks.length; i++) {
      var t = normalizeTask(tasks[i], i);
      var p = t.plant || 'Default Plant';
      var l = t.line || 'Line 1';
      if (!plants[p]) plants[p] = {};
      if (!plants[p][l]) plants[p][l] = [];
      plants[p][l].push(t);
    }
    return plants;
  }

  function renderGantt(tasks) {
    if (!ganttHierarchyEl) return;
    ganttHierarchyEl.innerHTML = '';
    if (!tasks || tasks.length === 0) {
      ganttHierarchyEl.innerHTML =
        '<div class="gantt-empty">No tasks to display. Click "Get production plan" or ask for a plan in chat.</div>';
      return;
    }
    var visLib = window.vis;
    if (!visLib || typeof visLib.Timeline !== 'function' || typeof visLib.DataSet !== 'function') {
      ganttHierarchyEl.innerHTML =
        '<div class="gantt-empty">Timeline library failed to load. Check browser console and refresh.</div>';
      return;
    }

    var hierarchy = buildHierarchy(tasks);
    var plantNames = Object.keys(hierarchy).sort();
    var timelineIdx = 0;

    for (var pi = 0; pi < plantNames.length; pi++) {
      var plantName = plantNames[pi];
      var plantDiv = document.createElement('div');
      plantDiv.className = 'hier-plant';

      var plantHeader = document.createElement('div');
      plantHeader.className = 'hier-header hier-plant-header';
      plantHeader.innerHTML =
        '<span class="hier-toggle">&#9660;</span> <strong>' + plantName + '</strong>';
      plantDiv.appendChild(plantHeader);

      var plantBody = document.createElement('div');
      plantBody.className = 'hier-body';
      plantHeader.addEventListener('click', (function (body, header) {
        return function () {
          var collapsed = body.style.display === 'none';
          body.style.display = collapsed ? '' : 'none';
          header.querySelector('.hier-toggle').innerHTML = collapsed ? '&#9660;' : '&#9654;';
        };
      })(plantBody, plantHeader));

      var lineNames = Object.keys(hierarchy[plantName]).sort();
      for (var li = 0; li < lineNames.length; li++) {
        var lineName = lineNames[li];
        var lineTasks = hierarchy[plantName][lineName] || [];

        var machineSet = {};
        lineTasks.forEach(function (t) {
          if (t.machine) machineSet[t.machine] = t.machine_name || t.machine;
        });
        var machineIds = Object.keys(machineSet).sort();

        var lineDiv = document.createElement('div');
        lineDiv.className = 'hier-line';

        var lineHeader = document.createElement('div');
        lineHeader.className = 'hier-header hier-line-header';
        lineHeader.innerHTML =
          '<span class="hier-toggle">&#9660;</span> <strong>' +
          lineName + '</strong> <span class="hier-task-count">' +
          machineIds.length + ' machine(s), ' + lineTasks.length + ' block(s)</span>';
        lineDiv.appendChild(lineHeader);

        var lineBody = document.createElement('div');
        lineBody.className = 'hier-body';
        lineHeader.addEventListener('click', (function (body, header) {
          return function () {
            var collapsed = body.style.display === 'none';
            body.style.display = collapsed ? '' : 'none';
            header.querySelector('.hier-toggle').innerHTML = collapsed ? '&#9660;' : '&#9654;';
          };
        })(lineBody, lineHeader));

        var timelineId = 'timeline-line-' + timelineIdx++;
        var wrap = document.createElement('div');
        wrap.className = 'hier-gantt-wrap';
        var timelineDiv = document.createElement('div');
        timelineDiv.id = timelineId;
        timelineDiv.className = 'hier-gantt vis-timeline-container';
        wrap.appendChild(timelineDiv);
        lineBody.appendChild(wrap);
        lineDiv.appendChild(lineBody);
        plantBody.appendChild(lineDiv);

        var groups = new visLib.DataSet(
          machineIds.map(function (mid) {
            return { id: mid, content: machineSet[mid] || mid };
          })
        );

        var items = new visLib.DataSet(
          lineTasks.map(function (t, idx) {
            var nt = normalizeTask(t, idx);
            return {
              id: nt.id,
              group: nt.machine || 'Unknown',
              content: nt.name,
              start: nt.start,
              end: nt.end,
              className: getTimelineClassName(nt),
              type: 'range',
              title: popupHtml(nt),
              data: nt,
              style: 'background-color:' + nt.color + ';border-color:' + nt.color + ';color:#fff;'
            };
          })
        );

        (function (el, itemsDs, groupsDs) {
          var startMin = itemsDs.min('start');
          var endMax = itemsDs.max('end');
          var options = {
            stack: false,
            groupOrder: 'id',
            margin: { item: { horizontal: 0 } },
            zoomKey: 'ctrlKey',
            orientation: 'top',
            tooltip: { followMouse: true, overflowMethod: 'cap' }
          };
          if (startMin && endMax) {
            options.start = startMin.start;
            options.end = endMax.end;
          }
          var tl = new visLib.Timeline(el, itemsDs, groupsDs, options);
          tl.on('select', function (properties) {
            if (properties.items && properties.items.length > 0) {
              var item = itemsDs.get(properties.items[0]);
              if (item && item.data && typeof GanttDetail !== 'undefined') {
                GanttDetail.onBlockClick(item.data);
              }
            }
          });
        })(timelineDiv, items, groups);
      }
      plantDiv.appendChild(plantBody);
      ganttHierarchyEl.appendChild(plantDiv);
    }
  }

  function renderSalesOrdersByMaterial(items) {
    if (!salesOrdersEl) return;
    if (!items || !items.length) {
      salesOrdersEl.innerHTML = '<div class="sales-empty">No sales order data found.</div>';
      return;
    }
    items = items.slice().sort(function (a, b) {
      var ad = a.earliest_due || '9999-12-31';
      var bd = b.earliest_due || '9999-12-31';
      if (ad < bd) return -1;
      if (ad > bd) return 1;
      return (b.total_qty || 0) - (a.total_qty || 0);
    });
    var html = '<div class="sales-table-wrap"><table class="sales-table">';
    html += '<thead><tr><th>Material</th><th>Parent material</th><th>Finished product</th><th>Total qty</th><th>Orders</th><th>Earliest due</th><th>Latest due</th><th>Allergens</th></tr></thead><tbody>';
    for (var i = 0; i < items.length; i++) {
      var it = items[i] || {};
      var allergensHtml = (it.allergens && it.allergens.length) ? it.allergens.map(allergenBadgeHtml).join('') : '';
      html += '<tr>' +
        '<td>' + (it.material || '') + '</td>' +
        '<td>' + (it.parent_material || '') + '</td>' +
        '<td>' + (it.finished_product || '') + '</td>' +
        '<td class="num">' + (typeof it.total_qty === 'number' ? it.total_qty.toFixed(0) : (it.total_qty || '0')) + '</td>' +
        '<td class="num">' + (it.orders || 0) + '</td>' +
        '<td>' + (it.earliest_due || '') + '</td>' +
        '<td>' + (it.latest_due || '') + '</td>' +
        '<td>' + allergensHtml + '</td></tr>';
    }
    html += '</tbody></table></div>';
    salesOrdersEl.innerHTML = html;
  }

  function renderInventorySummary(summary) {
    if (!inventorySummaryEl) return;
    if (!summary) {
      inventorySummaryEl.innerHTML = '<div class="empty-state">No inventory data available.</div>';
      return;
    }
    var html = '';
    html += '<div class="summary-card"><div class="value">' + (summary.total_materials || 0) + '</div><div class="label">Total Materials</div></div>';
    html += '<div class="summary-card success"><div class="value">' + (summary.sufficient_count || 0) + '</div><div class="label">Sufficient</div></div>';
    var shortageClass = summary.shortage_count > 0 ? 'error' : 'success';
    html += '<div class="summary-card ' + shortageClass + '"><div class="value">' + (summary.shortage_count || 0) + '</div><div class="label">Shortages</div></div>';
    inventorySummaryEl.innerHTML = html;
  }

  function renderMaterialShortages(shortages) {
    if (!materialShortagesEl) return;
    if (!shortages || shortages.length === 0) {
      materialShortagesEl.innerHTML = '<div class="empty-state">No material shortages detected.</div>';
      return;
    }
    var html = '';
    shortages.forEach(function(s) {
      var criticalClass = s.shortage > s.available ? 'critical' : '';
      html += '<div class="shortage-item ' + criticalClass + '">';
      html += '<div class="material-name">' + (s.material_name || s.material_id || s.ingredient_name || 'Unknown') + '</div>';
      html += '<div class="shortage-details">';
      html += 'Required: ' + (s.required || s.required_qty_lbs || 0) + ' | ';
      html += 'Available: ' + (s.available || s.stock_on_hand_lbs || 0) + ' | ';
      html += '<strong>Shortage: ' + (s.shortage || s.shortage_lbs || 0) + '</strong>';
      html += '</div>';
      if (s.affected_products && s.affected_products.length > 0) {
        html += '<div class="shortage-details">Affects: ' + s.affected_products.join(', ') + '</div>';
      }
      html += '</div>';
    });
    materialShortagesEl.innerHTML = html;
  }

  function renderMachineSchedules(schedules) {
    if (!machineSchedulesEl) return;
    if (!schedules || schedules.length === 0) {
      machineSchedulesEl.innerHTML = '<div class="empty-state">No machine schedules available.</div>';
      return;
    }
    var html = '';
    schedules.forEach(function(m) {
      html += '<div class="machine-item">';
      html += '<div class="machine-name">' + (m.machine_name || m.machine_id || 'Unknown') + '</div>';
      if (m.task_sequence && m.task_sequence.length > 0) {
        html += '<div class="task-list">Tasks: ' + m.task_sequence.length + ' (' + m.task_sequence.slice(0, 5).join(', ');
        if (m.task_sequence.length > 5) html += '...';
        html += ')</div>';
      }
      if (m.reasoning) {
        html += '<div class="task-list"><em>' + m.reasoning + '</em></div>';
      }
      if (m.cleaning_events && m.cleaning_events.length > 0) {
        html += '<div class="cleaning-info">' + m.cleaning_events.length + ' cleaning event(s) scheduled</div>';
      }
      html += '</div>';
    });
    machineSchedulesEl.innerHTML = html;
  }

  function renderValidationIssues(issues) {
    if (!validationIssuesEl) return;
    if (!issues || issues.length === 0) {
      validationIssuesEl.innerHTML = '<div class="empty-state">No validation issues found. Schedule looks good!</div>';
      return;
    }
    var html = '';
    issues.forEach(function(v) {
      var severityClass = v.severity === 'error' ? 'error' : 'warning';
      html += '<div class="validation-item ' + severityClass + '">';
      html += '<div class="issue-type">' + (v.issue_type || v.type || 'Issue').replace(/_/g, ' ') + '</div>';
      html += '<div class="issue-message">' + (v.message || 'Unknown issue') + '</div>';
      html += '</div>';
    });
    validationIssuesEl.innerHTML = html;
  }

  function renderSchedulingSummary(summary) {
    if (!summaryPanelEl) return;
    if (!summary || !summary.total_process_orders) {
      summaryPanelEl.style.display = 'none';
      return;
    }
    summaryPanelEl.style.display = 'block';
    summaryPanelEl.innerHTML =
      '<h2>Scheduling Summary</h2>' +
      '<div class="summary-grid">' +
        '<div class="stat"><label>Process Orders</label><value>' + (summary.total_process_orders || 0) + '</value></div>' +
        '<div class="stat"><label>Scheduled</label><value class="green">' + (summary.scheduled || 0) + '</value></div>' +
        '<div class="stat"><label>MRP Blocked</label><value class="red">' + (summary.blocked_mrp || 0) + '</value></div>' +
        '<div class="stat"><label>Missed Date Risk</label><value class="amber">' + (summary.missed_date_risk || 0) + '</value></div>' +
        '<div class="stat"><label>Cleaning Blocks</label><value class="orange">' + (summary.total_cleaning_blocks || 0) + '</value></div>' +
        '<div class="stat"><label>Allergen CIPs</label><value class="orange">' + (summary.allergen_cip_count || 0) + '</value></div>' +
        '<div class="stat"><label>Machines</label><value>' + (summary.machines_scheduled || 0) + '</value></div>' +
        '<div class="stat"><label>Exceptions</label><value class="' + (summary.critical_exceptions > 0 ? 'red' : '') + '">' + (summary.exceptions_total || 0) + '</value></div>' +
      '</div>';
  }

  function renderAllergenWarnings(warnings) {
    if (!allergenWarningsPanelEl) return;
    var targetEl = allergenWarningsEl || allergenWarningsPanelEl;
    if (!warnings || warnings.length === 0) {
      if (allergenWarningsEl) allergenWarningsEl.innerHTML = '<div class="empty-state">No allergen changeovers this week.</div>';
      allergenWarningsPanelEl.style.display = 'none';
      return;
    }
    allergenWarningsPanelEl.style.display = 'block';
    var html = '<h2>Allergen Changeover Warnings (' + warnings.length + ')</h2>';
    warnings.forEach(function(w) {
      var atpCls = w.atp_required ? 'atp-required' : '';
      var fromAllergens = (w.from_allergens || []).join(',');
      var toAllergens = (w.to_allergens || []).join(',');
      html += '<div class="allergen-warning ' + atpCls + '">' +
        '<strong>' + (w.machine_id || '?') + '</strong>: ' +
        fromAllergens + ' &rarr; ' + toAllergens + ' | ' +
        (w.clean_type || 'CLEAN') + ' (' + (w.duration_min || 0) + ' min)' +
        (w.atp_required ? ' | <span style="color:#c62828;font-weight:600;">ATP SWAB HOLD</span>' : '') +
        (w.start ? ' | ' + w.start : '') +
        '</div>';
    });
    allergenWarningsPanelEl.innerHTML = html;

    if (allergenWarningsEl) {
      allergenWarningsEl.innerHTML = warnings.map(function(w) {
        var atpCls = w.atp_required ? 'atp-required' : '';
        var fromAllergens = (w.from_allergens || []).join(',');
        var toAllergens = (w.to_allergens || []).join(',');
        return '<div class="allergen-warning ' + atpCls + '">' +
          '<strong>' + (w.machine_id || '?') + '</strong>: ' +
          fromAllergens + ' &rarr; ' + toAllergens + ' | ' +
          (w.clean_type || 'CLEAN') + ' (' + (w.duration_min || 0) + ' min)' +
          (w.atp_required ? ' | <span style="color:#c62828;font-weight:600;">ATP SWAB HOLD</span>' : '') +
          '</div>';
      }).join('');
    }
  }

  function renderExceptions(exceptions) {
    if (!exceptionsPanelEl) return;
    var targetEl = schedulingExceptionsEl || exceptionsPanelEl;
    if (!exceptions || exceptions.length === 0) {
      if (schedulingExceptionsEl) schedulingExceptionsEl.innerHTML = '<div class="empty-state text-green-400">No exceptions &mdash; plan is clean.</div>';
      exceptionsPanelEl.style.display = 'none';
      return;
    }
    exceptionsPanelEl.style.display = 'block';
    var html = '<h2>Scheduling Exceptions (' + exceptions.length + ')</h2>';
    html += exceptions.map(function(e) {
      var sev = (e.severity || 'medium').toLowerCase();
      return '<div class="exception-item severity-' + sev + '">' +
        '<span class="badge">' + (e.severity || 'MEDIUM') + '</span>' +
        '<span class="type">' + (e.type || 'UNKNOWN') + '</span>' +
        '<span class="msg">' + (e.message || '') + '</span>' +
        '</div>';
    }).join('');
    exceptionsPanelEl.innerHTML = html;

    if (schedulingExceptionsEl) {
      schedulingExceptionsEl.innerHTML = exceptions.map(function(e) {
        var sev = (e.severity || 'medium').toLowerCase();
        return '<div class="exception-item severity-' + sev + '">' +
          '<span class="badge">' + (e.severity || 'MEDIUM') + '</span>' +
          '<span class="type">' + (e.type || 'UNKNOWN') + '</span>' +
          '<span class="msg">' + (e.message || '') + '</span>' +
          '</div>';
      }).join('');
    }
  }

  function renderRiskBanner(riskLevel, schedulingSummary) {
    if (!riskBannerEl || !riskTextEl) return;
    if (riskLevel === 'low') {
      riskBannerEl.style.display = 'none';
      return;
    }
    riskBannerEl.style.display = 'block';
    riskBannerEl.className = 'risk-banner ' + (riskLevel === 'high' ? 'high' : '');
    var text = '';
    if (riskLevel === 'high') {
      text = 'HIGH RISK: Some deliveries may be delayed. Review validation issues.';
    } else if (riskLevel === 'medium') {
      text = 'MEDIUM RISK: Some tasks have tight delivery timelines.';
    }
    if (schedulingSummary) {
      if (schedulingSummary.missed_date_risk > 0) {
        text += ' (' + schedulingSummary.missed_date_risk + ' date miss risk)';
      }
      if (schedulingSummary.critical_exceptions > 0) {
        text += ' | ' + schedulingSummary.critical_exceptions + ' critical exception(s)';
      }
    }
    riskTextEl.textContent = text;
  }

  function renderAllPlanningData(data) {
    renderInventorySummary(data.inventory_summary);
    renderMaterialShortages(data.material_shortages);
    renderMachineSchedules(data.machine_schedules);
    renderValidationIssues(data.validation_issues);
    renderSchedulingSummary(data.scheduling_summary);
    renderAllergenWarnings(data.allergen_warnings);
    renderExceptions(data.scheduling_exceptions);
    renderRiskBanner(data.risk_level, data.scheduling_summary);
  }

  function initDetailPanel(data) {
    if (typeof GanttDetail === 'undefined') return;
    GanttDetail.init({
      sales_orders:    data.sales_orders || [],
      recipes:         data.recipes || [],
      recipe_bom:      data.recipe_bom || [],
      inventory:       (data.inventory_summary && data.inventory_summary.items) || [],
      machines:        data.machines_info || [],
      allergen_matrix: data.allergen_warnings || [],
      cip_procedures:  data.cip_procedures || []
    });
  }

  chatSend.addEventListener('click', async function () {
    var text = (chatInput.value || '').trim();
    if (!text) return;
    chatInput.value = '';
    appendMessage(text, 'user');
    try {
      var data = await postChat(text);
      appendMessage(data.response || 'No response.', 'bot');
      if (data.plan_tasks && data.plan_tasks.length) renderGantt(data.plan_tasks);
      if (data.sales_orders_by_material) renderSalesOrdersByMaterial(data.sales_orders_by_material);
      renderAllPlanningData(data);
      initDetailPanel(data);
    } catch (e) {
      appendMessage('Error: ' + (e.message || e), 'bot');
    }
  });

  chatInput.addEventListener('keydown', function (e) {
    if (e.key === 'Enter') chatSend.click();
  });

  btnPlan.addEventListener('click', async function () {
    appendMessage('Loading production plan...', 'bot');
    try {
      var planBody = { message: 'Get production plan from selected datasources.' };
      var ids = getSelectedDatasourceIds();
      if (ids && ids.length) planBody.datasource_ids = ids;
      var res = await fetch(API_BASE + '/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(planBody)
      });
      if (!res.ok) throw new Error(res.statusText);
      var data = await res.json();
      if (data.response) appendMessage(data.response, 'bot');
      if (data.plan_tasks && data.plan_tasks.length) {
        renderGantt(data.plan_tasks);
      } else {
        renderGantt([]);
      }
      if (data.sales_orders_by_material) renderSalesOrdersByMaterial(data.sales_orders_by_material);
      renderAllPlanningData(data);
      initDetailPanel(data);
    } catch (e) {
      appendMessage('Error: ' + (e.message || e), 'bot');
      renderGantt([]);
    }
  });

  renderGantt([]);

  function restoreDatasourceSelection() {
    if (!chatDatasourceEl) return;
    for (var i = 0; i < chatDatasourceEl.options.length; i++) {
      var opt = chatDatasourceEl.options[i];
      opt.selected = false;
      if (lastSelectedDatasourceIds === null || lastSelectedDatasourceIds.length === 0) {
        if (opt.value === '') opt.selected = true;
      } else if (lastSelectedDatasourceIds.indexOf(parseInt(opt.value, 10)) !== -1) {
        opt.selected = true;
      }
    }
  }

  async function loadChatDatasourceOptions() {
    if (!chatDatasourceEl || !chatDatasourceWrap || !chatNoDsMsg) return;
    try {
      var res = await fetch(API_BASE + '/datasources');
      var items = await res.json();
      if (!items || items.length === 0) {
        chatDatasourceWrap.style.display = 'none';
        chatNoDsMsg.style.display = 'block';
        setChatEnabled(false);
        return;
      }
      chatNoDsMsg.style.display = 'none';
      chatDatasourceWrap.style.display = 'block';
      setChatEnabled(true);
      chatDatasourceEl.innerHTML = '<option value="">All</option>';
      items.forEach(function (ds) {
        var opt = document.createElement('option');
        opt.value = ds.id;
        opt.textContent = (ds.name || 'Unnamed') + ' (' + (ds.type || '') + ')';
        chatDatasourceEl.appendChild(opt);
      });
      restoreDatasourceSelection();
    } catch (e) {
      chatDatasourceWrap.style.display = 'none';
      chatNoDsMsg.style.display = 'block';
      setChatEnabled(false);
    }
  }

  if (chatDatasourceEl) {
    chatDatasourceEl.addEventListener('change', saveDatasourceSelection);
  }
  var goAddDsBtn = document.getElementById('chat-go-add-datasource');
  if (goAddDsBtn) {
    goAddDsBtn.addEventListener('click', function () {
      document.querySelector('.tab[data-tab="datasources"]').click();
    });
  }
  loadChatDatasourceOptions();

  // Tabs
  document.querySelectorAll('.tab').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var tab = this.getAttribute('data-tab');
      document.querySelectorAll('.tab').forEach(function (b) { b.classList.remove('active'); });
      document.querySelectorAll('.panel').forEach(function (p) { p.classList.remove('active'); });
      this.classList.add('active');
      var panel = document.getElementById('panel-' + tab);
      if (panel) panel.classList.add('active');
      if (tab === 'datasources') loadDatasources();
      if (tab === 'chat') loadChatDatasourceOptions();
    });
  });

  // Datasources
  function getDsForm() {
    var type = (document.getElementById('ds-type').value || 'postgres').toLowerCase();
    var port = document.getElementById('ds-port').value;
    if (!port && type === 'postgres') port = '5432';
    if (!port && type === 'clickhouse') port = '8123';
    if (!port && type === 'mysql') port = '3306';
    return {
      name: document.getElementById('ds-name').value.trim(),
      type: type,
      host: document.getElementById('ds-host').value.trim() || 'localhost',
      port: port ? parseInt(port, 10) : null,
      database: document.getElementById('ds-database').value.trim() || null,
      username: document.getElementById('ds-username').value.trim() || null,
      password: document.getElementById('ds-password').value || null
    };
  }

  function showDsMessage(msg, isError) {
    var el = document.getElementById('ds-form-message');
    el.textContent = msg;
    el.className = 'form-message ' + (isError ? 'error' : 'success');
  }

  document.getElementById('ds-test').addEventListener('click', async function () {
    var body = getDsForm();
    if (!body.type) { showDsMessage('Select a type', true); return; }
    try {
      var res = await fetch(API_BASE + '/datasources/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      var data = await res.json();
      if (data.success) showDsMessage('Connection successful.');
      else showDsMessage(data.error || 'Connection failed', true);
    } catch (e) {
      showDsMessage('Error: ' + (e.message || e), true);
    }
  });

  document.getElementById('ds-save').addEventListener('click', async function () {
    var body = getDsForm();
    if (!body.name) { showDsMessage('Name is required', true); return; }
    if (!body.type) { showDsMessage('Type is required', true); return; }
    try {
      var res = await fetch(API_BASE + '/datasources', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      if (res.ok) {
        showDsMessage('Datasource saved.');
        loadDatasources();
      } else {
        var err = await res.json().catch(function () { return {}; });
        showDsMessage(err.detail || res.statusText || 'Save failed', true);
      }
    } catch (e) {
      showDsMessage('Error: ' + (e.message || e), true);
    }
  });

  async function loadDatasources() {
    var listEl = document.getElementById('ds-list');
    try {
      var res = await fetch(API_BASE + '/datasources');
      var items = await res.json();
      listEl.innerHTML = '';
      if (!items || items.length === 0) {
        listEl.innerHTML = '<li class="ds-info">No datasources. Add one above.</li>';
        return;
      }
      items.forEach(function (ds) {
        var li = document.createElement('li');
        li.innerHTML = '<span class="ds-info">' + (ds.name || 'Unnamed') + ' (' + (ds.type || '') + ')</span>' +
          '<button type="button" class="ds-delete" data-id="' + ds.id + '">Delete</button>';
        li.querySelector('.ds-delete').addEventListener('click', function () {
          if (!confirm('Remove this datasource?')) return;
          fetch(API_BASE + '/datasources/' + ds.id, { method: 'DELETE' })
            .then(function (r) {
              if (r.ok) loadDatasources();
              else showDsMessage('Delete failed', true);
            })
            .catch(function (e) { showDsMessage('Error: ' + e.message, true); });
        });
        listEl.appendChild(li);
      });
    } catch (e) {
      listEl.innerHTML = '<li class="form-message error">Failed to load: ' + (e.message || e) + '</li>';
    }
  }
})();
