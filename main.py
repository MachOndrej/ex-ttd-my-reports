import logging
from typing import List

from keboola.component.base import ComponentBase, sync_action
from keboola.component.sync_actions import SelectElement
from keboola.component import UserException
from keboola.utils.helpers import comma_separated_values_to_list

from ttd import TradeDesk


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self._logger = logging.getLogger(__name__)

    def run(self):
        params = self.configuration.parameters
        self._logger.debug(f"Running component with following params: {params}")
        ttd_client = TradeDesk(username=params.get("username"), password=params.get("#password"))
        advertisers = params.get("advertiser_ids")
        table_name = params.get("output_table_name")
        is_incremental = bool(params.get("incremental_output"))
        advertiser_ids = advertisers.split(",")
        report_url = ttd_client.get_report_url(report_schedule_id=params.get("report_schedule_id"),
                                               advertiser_ids=advertiser_ids)
        data = ttd_client.get_data(report_url)
        self._logger.info("Processing report.")

        table = self.create_out_table_definition(f"{table_name}.csv", incremental=is_incremental,
                                                 primary_key=params.get("primary_keys"))

        with open(table.full_path, mode="wt", encoding="utf-8", newline="") as out_file:
            out_file.write(data.text)

        self.write_manifest(table)

    def _get_advertisers(self) -> List[dict[str, str]]:
        params = self.configuration.parameters
        if isinstance(params.get("advertiser_id", ""), str):
            advertiser_ids = comma_separated_values_to_list(params.get("advertiser_id", ""))
        else:
            advertiser_ids = params.get("advertiser_id", [])
        if not advertiser_ids:
            ttd_client = TradeDesk(username=params.get("username"), password=params.get("#password"))
            partners = ttd_client.get_partners()
            advertiser_ids = ttd_client.get_advertisers(partners)

            return advertiser_ids
        if not advertiser_ids:
            advertiser_ids = []
        return advertiser_ids

    @sync_action("loadAdvertisers")
    def load_advertisers(self) -> List[SelectElement]:
        advertiser_ids = self._get_advertisers()
        select_elements = [SelectElement(value=adv["advertiser_id"], label=adv["advertiser_name"]) for adv in
                           advertiser_ids]

        return select_elements


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
