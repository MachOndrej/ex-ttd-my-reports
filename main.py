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
        ttd_client = TradeDesk(username=params.get("username"), password=params.get("#password"))
        ttd_client.get_report_url(report_schedule_id=params.get("report_schedule_id"),
                                  advertiser_id=params.get("advertiser_id"))
        data = ttd_client.get_data()

        is_incremental = bool(params.get("incremental_output"))

        table = self.create_out_table_definition("output.csv", incremental=is_incremental,
                                                 primary_key=params.get("primary_keys"))

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
