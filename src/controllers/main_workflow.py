

from .find_matches_process import MatchesProcess
from .op import OP
from datetime import date
import os
import sys
import logging

class WorkFlow:
    def start(self, config, start_date, end_date, tipo_doc):
        
        self.matches_process = MatchesProcess()
        result = self.matches_process.compare_data(config, start_date, end_date, tipo_doc)
        
        if result:
            #{
            # #     "update": update_results,
            # #     "delete": delete_results,
            # #     "create": add_results,
            # #     "total_success": sum(r.get("success", False) for r in update_results + delete_results + add_results),
            # #     "total_failed": sum(not r.get("success", False) for r in update_results + delete_results + add_results)
            # # }
            if len(result['api_operations']['create'])==0:
                logging.info('Nothing to send...')
                logging.info('Finish process...')

            op = OP()
            op.execute(result['api_operations'], tipo_doc)
            

        return  result 