import re
import json
import aiohttp
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup, Tag
from fake_useragent import UserAgent
from submodule_integrations.models.integration import Integration
from submodule_integrations.utils.errors import IntegrationAuthError, IntegrationAPIError


class BcBsAlIntegration(Integration):
    def __init__(self, user_agent: str = UserAgent().random):
        super().__init__("bcbsal")
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
            print(await response.text())
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
            self, contract_id: str, first_name: str, last_name: str, mid_init: str, dob: str,
            preservice_codes: list[str] = None
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
        self._scan_form_errors(health_benefit_page)

        health_benefit_soup = self._create_soup(health_benefit_page)
        bottom_content_tab = health_benefit_soup.select_one("div#ebBottomTabs")
        if bottom_content_tab is None:
            raise IntegrationAPIError(
                integration_name="bcbsal",
                error_code="server_error",
                status_code=500,
                message="Failed to load Coverage page content",
            )

        health_plan_element = health_benefit_soup.select_one("div#Covered-panel-1")
        health_plan_coverage = self._parse_insurance_table(health_plan_element)

        professional_office_element = health_benefit_soup.select_one("div#Covered-panel-14")
        professional_office_coverage = self._parse_insurance_table(professional_office_element)

        # Switch to Diagnostics Lab page
        update_path = self._get_element_data(selector="form#ebHeaderForm", key="action", soup=health_benefit_soup)
        update_data = {
            'submitType': '',
            'networkType': 'all',
            'selectedServiceOrDental': 'S',
            'serviceType': '5',
            'dateOfService': f'{self._get_current_date_formatted()}',
        }
        diagnostic_lab_page = await self._make_request(
            "POST", url=update_path, headers=headers, data=update_data
        )
        self._scan_form_errors(diagnostic_lab_page)

        diagnostic_lab_soup = self._create_soup(diagnostic_lab_page)
        diagnostic_lab_element = diagnostic_lab_soup.select_one("div#Covered-panel-2")
        diagnostic_lab_coverage = self._parse_insurance_table(diagnostic_lab_element)

        # Switch to diagnostics medical page
        update_data["serviceType"] = "73"
        diagnostic_medical_page = await self._make_request(
            "POST", url=update_path, headers=headers, data=update_data
        )
        self._scan_form_errors(diagnostic_medical_page)

        diagnostic_medical_soup = self._create_soup(diagnostic_medical_page)
        diagnostic_medical_element = diagnostic_medical_soup.select_one("div#Covered-panel-3")
        diagnostic_medical_coverage = self._parse_insurance_table(diagnostic_medical_element)

        # Switch to Medical Care page
        update_data["serviceType"] = "1"
        medical_care_page = await self._make_request(
            "POST", url=update_path, headers=headers, data=update_data
        )
        self._scan_form_errors(medical_care_page)

        medical_care_soup = self._create_soup(medical_care_page)
        medical_care_element = medical_care_soup.select_one("div#Covered-panel-9")
        medical_care_coverage = self._parse_insurance_table(medical_care_element)

        iv_therapy_element = medical_care_soup.select_one("div#Covered-panel-12")
        iv_therapy_coverage = self._parse_insurance_table(iv_therapy_element)

        preservice_data_list = []
        if preservice_codes is not None:
            auth_jwt = await self._get_cache_jwt()
            all_codes = await self._get_pre_service_codes(jwt=auth_jwt)

            for pr_code in preservice_codes:
                preservice_data = await self._get_pre_service_data(
                    code=pr_code,
                    codes_list=all_codes,
                    jwt=auth_jwt
                )
                preservice_data_list.append(preservice_data)

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
            "preservice": preservice_data_list,
        }
        return result

    async def get_preservice_codes(self):
        await self._get_eligibility_page()
        jwt = await self._get_cache_jwt()
        all_codes = await self._get_pre_service_codes(jwt=jwt)

        return all_codes

    async def _get_cache_jwt(self):
        params = {
            'p_p_id': 'selector_WAR_paselectorportlet',
            'p_p_lifecycle': '2',
            'p_p_state': 'normal',
            'p_p_mode': 'view',
            'p_p_resource_id': 'generateBusinessToken',
            'p_p_cacheability': 'cacheLevelPage',
        }
        path = f"{self.url}/portal/group/pa/utilization-review-physician"
        headers = self.headers.copy()
        headers["X-Requested-With"] = "XMLHttpRequest"
        headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"

        response = await self._make_request("POST", url=path, headers=headers, data=params)
        return response

    async def _get_pre_service_codes(self, jwt: str):
        headers = self.headers.copy()
        headers["X-Requested-With"] = "XMLHttpRequest"
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = f"Bearer {jwt}"
        headers["Accept"] = "application/json"

        path = f"{self.url}/pa-medical-coding-ws/data/getAllActiveProcedureCodes"

        response = await self._make_request("GET", url=path, headers=headers)
        return response

    async def _get_pre_service_data(self, code: str = None, jwt: str = None, codes_list: list = None):
        if code is None:
            return None

        code = code.upper()
        jwt_token = jwt if jwt is not None else await self._get_cache_jwt()
        preservice_codes = codes_list if codes_list is not None else await self._get_pre_service_codes(jwt_token)

        code_data: dict = next((item for item in preservice_codes if item.get("code") == code), None)
        if code_data is None:
            return f"{code} not found in codes list"

        path = f"{self.url}/portal/group/pa/utilization-review-physician"
        headers = self.headers.copy()
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

        pre_service_page = await self._make_request("GET", url=path, headers=headers)
        pre_service_page_soup = self._create_soup(pre_service_page)

        search_form_path = self._get_element_data(
            selector="form#precertificationSearchForm", key="action", soup=pre_service_page_soup
        )
        q0138_data = {
            'cptCodeDescription': f'{code_data.get("description")}',
            'cptCodeType': f'{code_data.get("codeType")}',
            'cptCode': f'{code}',
        }
        code_response: str = await self._make_request(
            "POST", url=search_form_path, headers=headers, data=q0138_data
        )
        q0138_soup = self._create_soup(code_response)
        unavailable_elem = q0138_soup.select_one("div#_precertification_WAR_paprecertificationportlet_ErrorDiv")
        if unavailable_elem:
            code_message = unavailable_elem.text.strip()
        else:
            sect_start = code_response.find("outpatientSetting")
            sect_end = code_response[sect_start:].find("cptCode")
            section_str = code_response[sect_start:sect_end + sect_start]
            section_json = self._extract_script_json(section_str)
            code_message = section_json.get("outpatientMessage")

        result = {
            "outpatient_data": code_message,
            "description": code_data.get("description"),
            "code": code,
        }
        return result

    @staticmethod
    def _parse_insurance_table(table_element):
        """
        Parse insurance coverage table into a hierarchical structure.

        Args:
            table_element (bs4.element.Tag): BeautifulSoup Tag containing the insurance coverage table

        Returns:
            dict: Hierarchical representation of the table data
        """
        # Find all table rows within the provided element
        rows = table_element.find_all('div', class_='eb-row')

        # Initialize the result structure
        result = {}

        current_section = None
        current_subsection = None
        current_subsection2 = None

        # Process each row
        for i, row in enumerate(rows):
            # Check first column (section) - look for parent section headers
            section_col = row.find('span', class_='table-div EBInfoCdbordertop') or row.find('span',
                                                                                             class_='table-div EBInfoCd')

            if section_col and section_col.find('div', class_='fonteb'):
                section_text = section_col.find('div', class_='fonteb').text.strip()
                if section_text:
                    current_section = section_text
                    # When a new section is found, initialize it in the result
                    if current_section not in result:
                        result[current_section] = {}
                    # Reset subsection tracking for this new section
                    current_subsection = None
                    current_subsection2 = None

            # Skip row processing if we don't have a current section
            if not current_section:
                continue

            # Check second column (subsection)
            subsection_col = row.find('span', class_=lambda c: c and 'NetworkType' in c)
            if subsection_col and subsection_col.find('div', class_='fonteb'):
                subsection_text = subsection_col.find('div', class_='fonteb').text.strip()
                if subsection_text:
                    current_subsection = subsection_text
                    if current_subsection not in result[current_section]:
                        result[current_section][current_subsection] = {}
                    # Reset subsection2 tracking
                    current_subsection2 = None

            # Skip further processing if we don't have a current subsection
            if not current_subsection:
                continue

            # Check third column (subsection2)
            subsection2_col = row.find('span', class_=lambda c: c and 'CovgLevelCd' in c)
            subsection2_text = None
            program_text = None

            if subsection2_col:
                divs = subsection2_col.find_all('div', class_='fonteb')
                # First div is usually the subsection2 (like "Individual")
                if divs and len(divs) > 0 and divs[0].text.strip():
                    subsection2_text = divs[0].text.strip()

                # Second div often contains program information
                if len(divs) > 1 and divs[1].text.strip():
                    program_text = divs[1].text.strip()

            # Update current_subsection2 if we found a valid value
            if subsection2_text:
                current_subsection2 = subsection2_text
                if current_subsection2 not in result[current_section][current_subsection]:
                    result[current_section][current_subsection][current_subsection2] = []

            # Extract data from remaining columns
            data = {}

            # Add program information if available
            if program_text:
                data['program'] = program_text

            # Get amount/value (4th column)
            qty_qual_col = row.find('span', class_=lambda c: c and 'QtyQualCd' in c)
            if qty_qual_col:
                divs = qty_qual_col.find_all('div', class_='fonteb')
                for div in divs:
                    if div.text.strip():
                        if '$' in div.text:
                            data['amount'] = div.text.strip()
                        elif '%' in div.text:
                            data['percentage'] = div.text.strip()

            # Get frequency/timeframe (5th column)
            quantity_col = row.find('span', class_=lambda c: c and 'Quantity' in c)
            if quantity_col:
                frequency_text = quantity_col.text.strip()
                if frequency_text:
                    data['frequency'] = frequency_text.strip()

            # Get precertification info (6th column)
            precert_col = row.find('span', class_=lambda c: c and 'PrecertCd' in c)
            if precert_col:
                # Extract main precertification text
                if precert_col.find('div', class_='fonteb'):
                    precert_text = precert_col.find('div', class_='fonteb').text.strip()
                    if precert_text:
                        data['precertification'] = precert_text

                # Look for "Benefit Begin" or other information in this column
                benefit_text = precert_col.text.strip()
                if benefit_text and "Benefit Begin" in benefit_text:
                    data['benefit_begin'] = benefit_text

            # Get messages/notes
            messages_col = row.find('span', class_=lambda c: c and 'Messages' in c)
            if messages_col and messages_col.find('ul'):
                messages = []
                for li in messages_col.find('ul').find_all('li'):
                    if li.text.strip():
                        messages.append(li.text.strip())

                if messages:
                    data['notes'] = messages

            # Add data to the appropriate location in the hierarchy
            if data:
                if current_subsection2:
                    # We have a specific subsection2, add data there
                    result[current_section][current_subsection][current_subsection2].append(data)
                else:
                    # We don't have subsection2 info, so create a default one
                    default_key = program_text if program_text else "Other"
                    if default_key not in result[current_section][current_subsection]:
                        result[current_section][current_subsection][default_key] = []
                    result[current_section][current_subsection][default_key].append(data)

        return result

    @staticmethod
    def group_by_category(parsed_data):
        """
        Group the parsed data by category for easier access

        Args:
            parsed_data (list): List of row dictionaries

        Returns:
            dict: Data organized by category
        """
        grouped_data = {}

        for row in parsed_data:
            category = row.get('category')
            if not category:
                continue

            if category not in grouped_data:
                grouped_data[category] = []

            grouped_data[category].append(row)

        return grouped_data

    @staticmethod
    def _create_soup(html):
        return BeautifulSoup(html, "html.parser")

    @staticmethod
    def _get_element_data(selector: str, key: str, soup: BeautifulSoup):
        input_item = soup.select_one(selector)
        return input_item.get(key)

    @staticmethod
    def _get_current_date_formatted():
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

    @staticmethod
    def _scan_form_errors(html_content):
        """
        Scans the HTML content for form errors and returns a combined dictionary of all errors.

        Args:
            html_content (str): HTML content to scan

        Returns:
            dict: Dictionary of all errors found with field ID as key (general errors use 'general_error' keys)
        """
        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')

        # Dictionary to store all errors
        errors = {}

        # Find all error panels and extract general error messages
        error_panels = soup.find_all(class_='panel-error')
        general_error_counter = 0
        for panel in error_panels:
            error_text = panel.get_text(strip=True)
            if error_text:
                errors[f'general_error_{general_error_counter}'] = {
                    'error_message': error_text,
                    'error_type': 'general'
                }
                general_error_counter += 1

        # Find all input fields with the 'error' class
        error_inputs = soup.find_all('input', class_='error')

        # Extract the field ID and error message (from title attribute)
        for input_field in error_inputs:
            field_id = input_field.get('id', 'unknown')
            error_message = input_field.get('title')

            # Skip fields with no error message
            if error_message is None:
                continue

            current_value = input_field.get('value', '')
            field_name = input_field.get('name', '')

            errors[field_id] = {
                'error_message': error_message,
                'current_value': current_value,
                'field_name': field_name,
                'error_type': 'field'
            }

        if errors != {}:
            raise IntegrationAPIError(
                integration_name="bcbsal",
                status_code=400,
                error_code="request_error",
                message=json.dumps(errors)
            )
