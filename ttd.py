import logging

import requests
from requests.exceptions import HTTPError
from keboola.component import UserException


class TradeDesk:

    def __init__(self, username: str, password: str, logger: logging.Logger = None) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.api_base_url = "https://api.thetradedesk.com/v3"
        self._authenticate()

    def _authenticate(self):
        auth_endpoint = "/authentication"
        self._logger.debug("Authenticating")
        payload = {
            "Login": self.username,
            "Password": self.password
        }
        response = self.session.post(self.api_base_url+auth_endpoint, json=payload)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._logger.error(f"Authentication failed: {exc}")
            raise UserException(exc)
        response = response.json()
        try:
            token = response["Token"]
            self._logger.debug("Token acquired successfully.")
        except KeyError as exc:
            self._logger.error(f"Failed to acquire token: {exc}")
            raise UserException(exc)
        self.session.headers.update(
            {
                "TTD-Auth": token
            }
        )
        self._logger.info("Authentication successful.")

    def get_report_url(self, report_schedule_id: str, advertiser_id: str) -> str:
        report_url = "/myreports/reportexecution/query/advertisers"
        payload = {
            "AdvertiserIds": [advertiser_id],
            "PageStartIndex": 0,
            "PageSize": 1,
            "ReportScheduleIds": [int(report_schedule_id)],
            "SortFields": [
                {
                    "FieldId": "ReportEndDateExclusive",
                    "ascending": False
                }
            ],
            "ReportExecutionStates": [
                "Complete"
            ]
        }
        self._logger.info(f"Retrieving scheduled report: {report_schedule_id}.")
        response = self.session.post(self.api_base_url+report_url, json=payload)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._logger.error(f"Retrieval failed: {exc}")
            raise UserException(exc)
        response = response.json()

        try:
            download_url = response["Result"][0]["ReportDeliveries"][0]["DownloadURL"]
            self._logger.info("Report download URL acquired.")
        except KeyError as exc:
            self._logger.error(f"Download URL missing: {exc}.")
            raise UserException(exc)

        return download_url

    def get_data(self, download_url: str) -> requests.Response:
        self._logger.info("Retrieving report")
        response = self.session.get(download_url)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._logger.error(f"Report retrieval failed: {exc}")
            raise UserException(exc)

        return response

    def get_partners(self) -> list[str]:
        self._logger.debug("Retrieving partners")
        payload = {
            "PageStartIndex": 0,
            "PageSize": 100
        }
        response = self.session.post(self.api_base_url+"/partner/query", json=payload)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            self._logger.error(f"Partner retrieval failed: {exc}")
            raise UserException(exc)
        resp = response.json()
        try:
            resp_result = resp["Result"]
        except KeyError as e:
            self._logger.error(f"No partners found")
            raise e
        partners = [
            resp["PartnerId"]
            for resp in resp_result
        ]

        return partners

    def get_advertisers(self, partner_ids: list[str]) -> list[dict[str, str]]:
        self._logger.debug("Retrieving advertisers")
        all_advertisers = []
        for partner_id in partner_ids:
            payload = {
                "PageStartIndex": 0,
                "PageSize": 100,
                "PartnerId": partner_id
            }
            response = self.session.post(self.api_base_url+"/advertiser/query/partner", json=payload)
            try:
                response.raise_for_status()
            except HTTPError as exc:
                self._logger.error(f"Advertiser retrieval failed: {exc}")
                raise UserException(exc)
            resp = response.json()
            try:
                resp_result = resp["Result"]
            except KeyError as e:
                self._logger.error(f"No advertiser found for partner_id: {partner_id}")
                raise e
            advertisers = [
                {"advertiser_name": resp["AdvertiserName"], "advertiser_id": resp["AdvertiserId"]}
                for resp in resp_result
            ]
            all_advertisers.extend(advertisers)

        return all_advertisers


