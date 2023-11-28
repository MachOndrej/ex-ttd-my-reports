import logging

from keboola.component.base import ComponentBase 
from keboola.component import UserException

from ttd import TradeDesk


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
    
    def run(self):
        params = self.configuration.parameters
        logging.info(params)
        ttd_client = TradeDesk(username=params.get("USERNAME"), password=params.get("#PASSWORD"))
        ttd_client.get_report_url(report_schedule_id=params.get("REPORT_SCHEDULE_ID"), advertiser_id=params.get("ADVERTISER_ID"))
        data = ttd_client.get_data()

        table = self.create_out_table_definition("output.csv", incremental=False)
        out_table_path = table.full_path

        with open(table.full_path, mode="wt", encoding="utf-8", newline="") as out_file:
            out_file.write(data.text)

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
    