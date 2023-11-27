import json
import requests

class TradeDesk:

    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.api_base_url = "https://api.thetradedesk.com/v3"
        self._authenticate()

    def _authenticate(self):
        auth_endpoint = "/authentication"
        payload = json.dumps({
            "Login": self.username,
            "Password": self.password
        })
        response = self.session.post(self.api_base_url+auth_endpoint, data=payload)
        response = response.json()
        token = response["Token"]
        self.session.headers.update(
            {
                "TTD-Auth": token
            }
        )

    def get_report_url(self, report_schedule_id: str, advertiser_id: str):
        report_url = "/myreports/reportexecution/query/advertisers"
        payload = json.dumps({
            "AdvertiserIds": [advertiser_id],
            "PageStartIndex": 0,
            "PageSize": 1,
            "ReportScheduleIds": [report_schedule_id],
            "SortFields": [
                {
                    "FieldId": "ReportEndDateExclusive",
                    "ascending": False
                }
            ],
            "ReportExecutionStates": [
                "Complete"
            ]
        })
        response = self.session.post(self.api_base_url+report_url, data=payload)
        response = response.json()
        self.download_url = response["Result"][0]["ReportDeliveries"][0]["DownloadURL"]

    def get_data(self) -> requests.Response:
        response = self.session.get(self.download_url)

        return response