"""Port (interface) for datasource persistence - implemented in infrastructure."""
from abc import ABC, abstractmethod
from typing import List, Optional

from src.domain.entities.datasource_entity import DatasourceEntity


class IDatasourceRepository(ABC):
    """Abstract repository for datasource metadata (psdb.datasources)."""

    @abstractmethod
    def add(self, datasource: DatasourceEntity) -> DatasourceEntity:
        """Persist a new datasource; return entity with id and created_at set."""
        pass

    @abstractmethod
    def get_all(self) -> List[DatasourceEntity]:
        """Return all datasources (passwords may be masked in application layer)."""
        pass

    @abstractmethod
    def get_by_id(self, id: int) -> Optional[DatasourceEntity]:
        """Return one datasource by id."""
        pass

    @abstractmethod
    def delete(self, id: int) -> bool:
        """Remove datasource by id; return True if deleted."""
        pass
