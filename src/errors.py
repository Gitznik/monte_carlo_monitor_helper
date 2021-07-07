class environmentError(Exception):
    def __init__(
            self, 
            variable: str, 
            message: str = 'Environment Variable not set up') -> None:
        self.variable = variable
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return f'{self.variable} -> {self.message}'

class monitoringError(Exception):
    def __init__(
            self,
            table_name: str,
            message: str) -> None:
        self.table_name = table_name
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return f'{self.table_name} -> {self.message}'

class configError(Exception):
    def __init__(
            self,
            config_part: str,
            message: str = 'incorrect setup') -> None:
        self.config_part = config_part
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return f'{self.config_part} -> {self.message}'