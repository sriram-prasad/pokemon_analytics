import requests
import json
import time
import logging

logging.basicConfig(
    level=logging.INFO, format=" %(asctime)s -  %(levelname)s -  %(message)s"
)


class Extraction:
    """
    This class is used to extract the data from the API
    """

    base_url = "https://pokeapi.co/api/v2/"

    def __init__(self, endpoint) -> None:
        self.api_url = self.base_url + endpoint

    def api_call(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from API: {e}")
            return None

    def write_data_to_file(self, data):
        with open("data/pokemon.json", "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)

    def extract_pokemon(self):
        """
        This method is used to extract the data from the API
        """
        url = self.api_url
        results = []

        start_time = time.time()
        logging.info(f"Start: {start_time}")

        while url:
            data = self.api_call(url)

            if data is None:
                break

            for result in data["results"]:
                stats_data = self.api_call(result["url"])
                if stats_data is None:
                    continue
                else:
                    stats_data = {
                        stat["stat"]["name"]: stat["base_stat"]
                        for stat in stats_data["stats"]
                    }
                    result["stats"] = stats_data
                    logging.info(result)

            results.extend(data["results"])

            url = data.get("next")

        end_time = time.time()
        logging.info(f"End: {end_time}")
        logging.info(f"Execution Time: {end_time - start_time}")

        self.write_data_to_file(results)


obj = Extraction("pokemon")
obj.extract_pokemon()
# obj.extract_stats()
