import requests
import json
import concurrent.futures
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

    @staticmethod
    def api_call(url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from API: {e}")
            return None

    @staticmethod
    def write_data_to_file(data):
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

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            while url:
                data = self.api_call(url)

                if data is None:
                    logging.error("Error fetching data from API")
                    break

                poke_data = data["results"]
                stats = executor.map(self.api_call, [item["url"] for item in poke_data])

                for pokemon in stats:
                    poke_info = {
                        "id": pokemon["id"],
                        "order": pokemon["order"],
                        "name": pokemon["forms"][0]["name"],
                        "stats": {
                            stat["stat"]["name"]: stat["base_stat"]
                            for stat in pokemon["stats"]
                        },
                        "types": [item["type"]["name"] for item in pokemon["types"]],
                        "height": pokemon["height"] / 10,  # Decimeters to meters
                        "weight": pokemon["weight"] / 10,  # Decagrams to kilograms
                        "species": pokemon["species"]["name"],
                    }
                    logging.info(poke_info)
                    results.append(poke_info)

                url = data["next"]

        end_time = time.time()
        logging.info(f"End: {end_time}")
        logging.info(f"Execution Time: {end_time - start_time}")

        self.write_data_to_file(results)


obj = Extraction("pokemon")
obj.extract_pokemon()
