import logging

from keboola.component.base import ComponentBase 
from keboola.component import UserException
from requests import Response

from ttd import TradeDesk


REQUIRED_PARAMETERS = [REPORT_SCHEDULE_ID, USERNAME, PASSWORD, ADVERTISER_ID]

class Component(ComponentBase):

    def __init__(self):
        super().__init__()
    
    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration_parameters
        ttd_client = TradeDesk(username=params.get(USERNAME), password=params.get(PASSWORD))
        ttd_client.get_report_url(report_schedule_id=params.get(REPORT_SCHEDULE_ID), advertiser_id=params.get(ADVERTISER_ID))
        data = ttd_client.get_data()

        table = self.create_out_table_definition("output.csv", incremental=False)
        out_table_path = table.full_path

        with open(table.full_path, mode="wb", encoding="utf-8", newline="") as out_file:
            out_file.write(data.content)

        self.write_manifest(table)


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
    