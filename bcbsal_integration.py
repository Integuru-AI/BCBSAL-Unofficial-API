import re
import json
import aiohttp
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup, Tag
from fake_useragent import UserAgent
from submodule_integrations.models.integration import Integration
from submodule_integrations.utils.errors import IntegrationAuthError, IntegrationAPIError


class BcBsAlIntegration(Integration):
    def __init__(self, user_agent: str = UserAgent().random):
        super().__init__("service_titan")
        self.url = "https://providers.bcbsal.org"
        self.user_agent = user_agent
        self.network_requester = None
        self.headers = None

    async def _make_request(self, method: str, url: str, **kwargs):
        if self.network_requester is not None:
            response = await self.network_requester.request(
                method, url, process_response=self._handle_response, **kwargs
            )
            return response
        else:
            async with aiohttp.ClientSession() as session:
                async with session.request(method, url, **kwargs) as response:
                    return await self._handle_response(response)

    async def _handle_response(
            self, response: aiohttp.ClientResponse
    ):
        if response.status == 200:
            try:
                data = await response.json()
            except (json.decoder.JSONDecodeError, aiohttp.ContentTypeError):
                data = await response.text()

                if "<title>login - provider.bcbsal.org</title>" in data:
                    raise IntegrationAuthError(
                        "BCBSAL: Auth failed",
                        401
                    )

                if "AlrtmsgsId" in data:
                    resp_soup = self._create_soup(data)
                    alert_div = resp_soup.select_one("div#AlrtmsgsId")
                    error_items = alert_div.select("td")
                    if len(error_items) > 0:
                        msg = "\n".join(error.text.strip() for error in error_items)
                        raise IntegrationAPIError(
                            self.integration_name,
                            f"BCBSAL: API Error: {msg}",
                            status_code=400,
                            error_code="error",
                        )

            return data

        if response.status == 401:
            raise IntegrationAuthError(
                "BCBSAL: Auth failed",
                response.status,
            )
        elif response.status == 400:
            raise IntegrationAPIError(
                self.integration_name,
                f"{response.reason}",
                response.status,
                response.reason,
            )
        else:
            raise IntegrationAPIError(
                self.integration_name,
                f"{await response.json()}",
                response.status,
                response.reason,
            )

    async def initialize(self, token: str, network_requester=None):
        self.headers = {
            "Host": "providers.bcbsal.org",
            "User-Agent": self.user_agent,
            "Cookie": token,
            "Accept-Encoding": "gzip",
        }
        self.network_requester = network_requester

    async def _get_eligibility_page(self):
        path = f"{self.url}/portal/group/pa/eligibility"
        headers = self.headers.copy()
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

        response = await self._make_request("GET", url=path, headers=headers)
        return response

    async def get_coverage_data(
            self, contract_id: str, first_name: str, last_name: str, mid_init: str, dob: str
    ):
        elig_start_page = await self._get_eligibility_page()
        eligibility_soup = self._create_soup(elig_start_page)

        business_name = self._get_element_data(selector="input#businessName", key="value", soup=eligibility_soup)
        billing_npi = self._get_element_data(selector="input#billingNpi", key="value", soup=eligibility_soup)
        tax_id = self._get_element_data(selector="input#taxId", key="value", soup=eligibility_soup)
        business_sys = self._get_element_data(selector="input#businessBscSys", key="value", soup=eligibility_soup)
        business_index = self._get_element_data(selector="input#businessListIndex", key="value", soup=eligibility_soup)
        provider_index = self._get_element_data(selector="input#providerListIndex", key="value", soup=eligibility_soup)
        provider_name = self._get_element_data(selector="input#provName", key="value", soup=eligibility_soup)
        provider_npi = self._get_element_data(selector="input#provNpi", key="value", soup=eligibility_soup)
        lifetime_provider_id = self._get_element_data(
            selector="input#lifetimeProviderId", key="value", soup=eligibility_soup
        )
        bsc_sys = self._get_element_data(selector="input#bscSys", key="value", soup=eligibility_soup)
        enable_bypass = self._get_element_data(selector="input#isByPassEnabled", key="value", soup=eligibility_soup)
        bypass_count = self._get_element_data(selector="input#byPassCount", key="value", soup=eligibility_soup)
        service_code = self._get_element_data(selector="input#serviceTypeCode", key="value", soup=eligibility_soup)

        elig_data = {
            'businessName': f'{business_name}',
            'billingNpi': f'{billing_npi}',
            'taxId': f'{tax_id}',
            'businessBscSys': f'{business_sys}',
            'businessListIndex': f'{business_index}',
            'providerListIndex': f'{provider_index}',
            'provName': f'{provider_name}',
            'provNpi': f'{provider_npi}',
            'lifetimeProviderId': f'{lifetime_provider_id}',
            'bscSys': f'{bsc_sys}',
            'isByPassEnabled': f'{enable_bypass}',
            'byPassCount': f'{bypass_count}',
            'serviceTypeCode': f'{service_code}',
            'patientFullName': '',
            'contractNrFinder': '',
            'socialSecurityNumber': '',
            'submitTyp': 'Continue',
            'business-name': '',
            'prov-name': '',
            'contractNr': f'{contract_id}',
            'firstNm': f'{first_name}',
            'midInit': f'{mid_init}',
            'lastNm': f'{last_name}',
            'dob': f'{dob}',
            'gender': '',
        }

        path = self._get_element_data(selector="form#selectorForm", key="action", soup=eligibility_soup)
        headers = self.headers.copy()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

        health_benefit_page = await self._make_request("POST", url=path, headers=headers, data=elig_data)
        health_benefit_soup = self._create_soup(health_benefit_page)
        health_plan_element = health_benefit_soup.select_one("div#Covered-panel-1")
        health_plan_coverage = self._parse_insurance_table(health_plan_element)

        professional_office_element = health_benefit_soup.select_one("div#Covered-panel-14")
        professional_office_coverage = self._parse_insurance_table(professional_office_element)

        # Switch to Diagnostics Lab page
        update_path = self._get_element_data(selector="form#ebHeaderForm", key="action", soup=eligibility_soup)
        update_data = {
            'submitType': '',
            'networkType': 'all',
            'selectedServiceOrDental': 'S',
            'serviceType': '5',
            'dateOfService': f'{self.get_current_date_formatted()}',
        }
        diagnostic_lab_page = await self._make_request(
            "POST", url=update_path, headers=headers, data=update_data
        )
        diagnostic_lab_soup = self._create_soup(diagnostic_lab_page)
        diagnostic_lab_element = diagnostic_lab_soup.select_one("div#Covered-panel-2")
        diagnostic_lab_coverage = self._parse_insurance_table(diagnostic_lab_element)

        # Switch to diagnostics medical page
        update_data["serviceType"] = "73"
        diagnostic_medical_page = await self._make_request(
            "POST", url=update_path, headers=headers, data=update_data
        )
        diagnostic_medical_soup = self._create_soup(diagnostic_medical_page)
        diagnostic_medical_element = diagnostic_medical_soup.select_one("div#Covered-panel-3")
        diagnostic_medical_coverage = self._parse_insurance_table(diagnostic_medical_element)

        # Switch to Medical Care page
        update_data["serviceType"] = "1"
        medical_care_page = await self._make_request(
            "POST", url=update_path, headers=headers, data=update_data
        )
        medical_care_soup = self._create_soup(medical_care_page)
        medical_care_element = medical_care_soup.select_one("div#Covered-panel-9")
        medical_care_coverage = self._parse_insurance_table(medical_care_element)

        iv_therapy_element = medical_care_soup.select_one("div#Covered-panel-12")
        iv_therapy_coverage = self._parse_insurance_table(iv_therapy_element)

        preservice_data = await self._get_pre_service_data()

        coverage_data = {
            "health_benefit": health_plan_coverage,
            "professional_office": professional_office_coverage,
            "diagnostic_lab": diagnostic_lab_coverage,
            "diagnostic_medical": diagnostic_medical_coverage,
            "medical_care": medical_care_coverage,
            "iv_therapy": iv_therapy_coverage,
        }

        result = {
            "coverage": coverage_data,
            "preservice": preservice_data,
        }
        return result

    async def _get_pre_service_data(self):
        path = f"{self.url}/portal/group/pa/utilization-review-physician"
        headers = self.headers.copy()
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

        pre_service_page = await self._make_request("GET", url=path, headers=headers)
        pre_service_page_soup = self._create_soup(pre_service_page)

        search_form_path = self._get_element_data(
            selector="form#precertificationSearchForm", key="action", soup=pre_service_page_soup
        )
        q0138_data = {
            'cptCodeDescription': 'Injection, Ferumoxytol, For Treatment of Iron Deficiency Anemia, 1 mg (Non-Esrd Use)',
            'cptCodeType': 'HCPC',
            'cptCode': 'Q0138',
        }
        q0138_response: str = await self._make_request(
            "POST", url=search_form_path, headers=headers, data=q0138_data
        )
        q0138_sect_start = q0138_response.find("outpatientSetting")
        q0138_sect_end = q0138_response[q0138_sect_start:].find("cptCode")
        q0138_sect = q0138_response[q0138_sect_start:q0138_sect_end+q0138_sect_start]
        q0138_json = self._extract_script_json(q0138_sect)
        q0138_message = q0138_json.get("outpatientMessage")

        j1756_data = {
            'cptCodeDescription': 'Injection, Iron Sucrose, 1 mg',
            'cptCodeType': 'HCPC',
            'cptCode': 'J1756',
        }
        j1756_response: str = await self._make_request(
            "POST", url=search_form_path, headers=headers, data=j1756_data
        )
        j1756_sect_start = j1756_response.find("outpatientSetting")
        j1756_sect_end = j1756_response[j1756_sect_start:].find("cptCode")
        j1756_sect = j1756_response[j1756_sect_start:j1756_sect_end+j1756_sect_start]
        j1756_json = self._extract_script_json(j1756_sect)
        j1756_message = j1756_json.get("outpatientMessage")

        result = {
            "q0138": q0138_message,
            "j1756": j1756_message,
        }
        return result

    @staticmethod
    def _parse_insurance_table(table_element: Tag):
        """
        Parse an insurance benefits HTML table using BeautifulSoup

        Args:
            table_element: BeautifulSoup element representing the insurance table

        Returns:
            Tuple containing:
                - DataFrame with parsed row data
                - Structured dictionary with hierarchical insurance information
        """
        # Initialize a list to store the parsed data
        parsed_data = []

        # Find all rows in the table
        rows = table_element.find_all('div', class_='eb-row')

        # Helper function to extract text from an element and clean it
        def extract_text(element):
            if element is None:
                return ""
            return element.get_text().strip()

        # Variables to track the current section being processed
        current_section = None
        current_subsection = None

        # Process each row
        for row in rows:
            row_data = {}

            # Extract benefit info (first column)
            benefit_info = row.find('span', class_='table-div EBInfoCd') or row.find('span', class_='table-div EBInfoCdbordertop')
            benefit_text = extract_text(benefit_info.find('div', class_='fonteb') if benefit_info else None)

            if benefit_text:
                current_section = benefit_text

            # Extract network type (second column)
            network_type = row.find('span', class_='table-div NetworkType') or row.find('span', class_='table-div NetworkTypebordertop')
            network_text = extract_text(network_type.find('div', class_='fonteb') if network_type else None)

            # Extract coverage level (third column)
            coverage_level = row.find('span', class_='table-div CovgLevelCd') or row.find('span', class_='table-div CovgLevelCdbordertop')
            coverage_text = extract_text(coverage_level.find('div', class_='fonteb') if coverage_level else None)

            if coverage_text:
                current_subsection = coverage_text

            # Extract additional coverage info if available
            coverage_details = []
            if coverage_level:
                for div in coverage_level.find_all('div', class_='fonteb'):
                    text = extract_text(div)
                    if text:
                        coverage_details.append(text)

            # Extract amount (fourth column)
            amount = row.find('span', class_='table-div QtyQualCd') or row.find('span', class_='table-div QtyQualCdbordertop')
            amount_text = extract_text(amount.find('div', class_='fonteb') if amount else None)

            # Extract period/quantity (fifth column)
            period = row.find('span', class_='table-div Quantity') or row.find('span', class_='table-div Quantitybordertop')
            period_text = extract_text(period.find('div', class_='fonteb') if period else None)

            # Extract benefit date or additional info (sixth column)
            precert = row.find('span', class_='table-div PrecertCd') or row.find('span', class_='table-div PrecertCdbordertop')
            benefit_date = None
            if precert:
                date_info = precert.find('div', string=lambda s: 'Benefit Begin' in s if s else False)
                if date_info:
                    benefit_date = date_info.get_text().strip()

            # Extract messages (last column)
            messages = row.find('span', class_='table-div Messages') or row.find('span', class_='table-div Messagesbordertop')
            message_items = []
            if messages:
                message_list = messages.find('ul', class_='fonteb')
                if message_list:
                    for li in message_list.find_all('li'):
                        message_items.append(li.get_text().strip())

            # Compile the row data
            row_data = {
                'Section': current_section,
                'Subsection': current_subsection,
                'Network Type': network_text,
                'Coverage Details': '; '.join(coverage_details) if coverage_details else None,
                'Amount': amount_text,
                'Period': period_text,
                'Benefit Date': benefit_date,
                'Messages': '; '.join(message_items) if message_items else None
            }

            # Add to our parsed data if we have meaningful information
            if any(value for value in row_data.values() if value):
                parsed_data.append(row_data)

        # Convert the parsed data to a DataFrame
        df = pd.DataFrame(parsed_data)

        # Clean up the data
        df = df.fillna('')

        # Create a more structured representation of the insurance information
        insurance_info = {}

        for _, row in df.iterrows():
            section = row['Section']
            subsection = row['Subsection']

            if section and section not in insurance_info:
                insurance_info[section] = {}

            if section:
                if subsection:
                    if subsection not in insurance_info[section]:
                        insurance_info[section][subsection] = {}

                    # Add the details
                    insurance_info[section][subsection].update({
                        'Amount': row['Amount'],
                        'Period': row['Period'],
                        'Network Type': row['Network Type'],
                        'Benefit Date': row['Benefit Date'],
                        'Messages': row['Messages'],
                        'Coverage Details': row['Coverage Details']
                    })
                else:
                    # For section-level data with no subsection
                    insurance_info[section].update({
                        'Network Type': row['Network Type'],
                        'Coverage Details': row['Coverage Details'],
                        'Messages': row['Messages']
                    })

        return insurance_info

    @staticmethod
    def _create_soup(html):
        return BeautifulSoup(html, "html.parser")

    @staticmethod
    def _get_element_data(selector: str, key: str, soup: BeautifulSoup):
        input_item = soup.select_one(selector)
        return input_item.get(key)

    @staticmethod
    def get_current_date_formatted():
        """
        Returns the current date in 'MM/DD/YYYY' format.
        """
        now = datetime.now()
        return now.strftime("%m/%d/%Y")

    @staticmethod
    def _extract_script_json(text) -> dict | None:
        """
        Extract the outpatientMessage value from a JavaScript variable assignment string.

        Args:
            text (str): The text containing the JavaScript variable assignment with JSON

        Returns:
            str: The extracted outpatientMessage value, or None if not found
        """
        # Use regex to find the JSON part within the string
        # Looking for JSON.parse('...') pattern
        match = re.search(r"JSON\.parse\('(.+?)'\)", text)

        if not match:
            return None

        try:
            # Extract the JSON string and parse it
            json_str = match.group(1)
            # Handle escaped quotes in the JSON string
            json_str = json_str.replace("\\'", "'")
            data = json.loads(json_str)

            # Extract the outpatientMessage if it exists
            return data
        except json.JSONDecodeError:
            return None
