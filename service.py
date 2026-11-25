"""服务层命名兼容入口。"""

from contract_hub import ContractHub as ContractService
from contract_hub import ContractHub

__all__ = ["ContractHub", "ContractService"]
