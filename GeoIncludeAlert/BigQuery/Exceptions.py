from pathlib import Path

import sys
import logging

# Setting Logging configurations
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

class InvalidCredentials(Exception):
    '''Invalid Credentials'''

    def __init__(self) -> None:
        self.messsage = f'''[-] Invalid Credentials'''
        logger.error(self.messsage)
        super().__init__(self.messsage)

class QueryFailed(Exception):
    '''Query Failed to Execute'''

    def __init__(self, query: str, error: Exception) -> None:
        self.message = f'''
            [-] Failed to execute query
            [-] Error : {error}
            [-] Failed Query : {query}
        '''
        logger.error(self.message)
        super().__init__(self.message)

class InvalidFilePath(Exception):
    '''Invalid File Path'''

    def __init__(self, file_path: Path) -> None:
        self.message = f'''[-] Invalid File Path: {file_path}'''
        logger.error(self.message)
        super().__init__(self.message)

class LoadJobFailed(Exception):
    '''BigQuery Load Table Job Failed'''

    def __init__(self, table_name: str, error: Exception): 
        self.message = f'''
            [-] Table Load Job Failed
            [-] Error : {error}
            [-] Destination Table : {table_name}
        '''
        logger.error(self.message)
        super().__init__(self.message)