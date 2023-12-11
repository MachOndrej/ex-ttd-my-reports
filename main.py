import logging
from typing import List

from keboola.component.base import ComponentBase, sync_action
from keboola.component.sync_actions import SelectElement
from keboola.component import UserException
from keboola.utils.helpers import comma_separated_values_to_list

from ttd import TradeDesk

PARAM_USERNAME = "username"
PARAM_PASSWORD = "#password"

PARAM_ADVERTISER_ID = "advertiser_id"
PARAM_PARTNER_ID = "partner_id"
PARAM_REPORT_LEVEL = "report_level"

PARAM_DESTINATION = "destination"
PARAM_DESTINATION_TABLE = "output_table_name"
PARAM_DESTINATION_PRIMARY_KEYS = "primary_keys"
PARAM_DESTINATION_INCREMENTAL = "incremental_output"

PARAM_REPORT_SETTINGS = "report_settings"
PARAM_REPORT_SETTINGS_SCHEDULE_ID = "report_schedule_id"


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self._logger = logging.getLogger(__name__)

    def run(self):
        self.validate_configuration_parameters()

        params = self.configuration.parameters
        self._logger.debug(f"Running component with following params: {params}")

        ttd_client = TradeDesk(username=params.get(PARAM_USERNAME), password=params.get(PARAM_PASSWORD))
        report_level = params.get(PARAM_REPORT_LEVEL)
        partner_ids = []
        advertiser_ids = []
        if report_level == 0:
            report_entity = "partners"
            partner_ids = self._get_partners()

        else:
            advertiser_ids = self._get_advertisers()
            report_entity = "advertisers"


        destination = params.get(PARAM_DESTINATION)
        table_name = destination.get(PARAM_DESTINATION_TABLE)
        is_incremental = bool(destination.get(PARAM_DESTINATION_INCREMENTAL))
        primary_keys = destination.get(PARAM_DESTINATION_PRIMARY_KEYS)

        report_settings = params.get(PARAM_REPORT_SETTINGS)
        report_schedule_id = report_settings.get(PARAM_REPORT_SETTINGS_SCHEDULE_ID)

        report_url = ttd_client.get_report_url(report_schedule_id=report_schedule_id,
                                               report_level=report_entity,
                                               advertiser_ids=advertiser_ids,
                                               partner_ids=partner_ids)
        data = ttd_client.get_data(report_url)
        self._logger.info("Processing report.")

        table = self.create_out_table_definition(f"{table_name}.csv", incremental=is_incremental,
                                                 primary_key=primary_keys)

        with open(table.full_path, mode="wt", encoding="utf-8", newline="") as out_file:
            out_file.write(data.text)

        self.write_manifest(table)

    def _get_partners(self) -> List[str]:
        params = self.configuration.parameters
        if isinstance(params.get(PARAM_PARTNER_ID, ""), str):
            partner_ids = comma_separated_values_to_list(params.get("partner_id", ""))
        else:
            partner_ids = params.get(PARAM_PARTNER_ID, [])
        if not partner_ids:
            ttd_client = TradeDesk(username=params.get(PARAM_USERNAME), password=params.get(PARAM_PASSWORD))
            partner_ids = ttd_client.get_partners()

            return partner_ids
        if not partner_ids:
            partner_ids = []
        return partner_ids

    def _get_advertisers(self) -> List[str]:
        params = self.configuration.parameters
        if isinstance(params.get(PARAM_ADVERTISER_ID, ""), str):
            advertiser_ids = comma_separated_values_to_list(params.get("advertiser_id", ""))
        else:
            advertiser_ids = params.get(PARAM_ADVERTISER_ID, [])
        if not advertiser_ids:
            ttd_client = TradeDesk(username=params.get(PARAM_USERNAME), password=params.get(PARAM_PASSWORD))
            partner_ids = ttd_client.get_partners()
            advertiser_ids = ttd_client.get_advertisers(partner_ids)

            return advertiser_ids
        if not advertiser_ids:
            advertiser_ids = []
        return advertiser_ids

    @sync_action("loadAdvertisers")
    def load_advertisers(self) -> List[SelectElement]:
        advertiser_ids = self._get_advertisers()
        select_elements = [SelectElement(value=advertiser, label=advertiser) for advertiser in
                           advertiser_ids]

        return select_elements

    @sync_action("loadPartners")
    def load_partners(self) -> List[SelectElement]:
        partners_ids = self._get_partners()
        select_elements = [SelectElement(value=partner, label=partner) for partner in
                           partners_ids]

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
