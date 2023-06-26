import requests
import json
import concurrent.futures
import time
from typing import Optional, Dict, List, Any
import logging

logging.basicConfig(
    filename="logs/extraction.log",
    level=logging.INFO,
    format="%(asctime)s -  %(levelname)s -  %(message)s",
)


class Extraction:
    """
    This class handles the extraction of Pokémon data from the PokéAPI.

    Attributes:
        pokemon_url (str): The full API endpoint for the data extraction of pokemons.
        type_url (str): The full API endpoint for the data extraction of types.
    """

    base_url: str = "https://pokeapi.co/api/v2/"

    def __init__(self) -> None:
        """
        Initialises the Extraction object with the complete API URL for the endpoint.
        """
        self.pokemon_url = self.base_url + "pokemon/"
        self.type_url = self.base_url + "type/"

    @staticmethod
    def api_call(url: str) -> Optional[Dict[str, Any]]:
        """
        Makes a GET request to the specified API URL and returns the response as a JSON object.
        In case of an HTTP error, it logs the exception and returns None.

        Args:
            url (str): The API URL for the GET request.

        Returns:
            Optional[Dict]: The response from the API converted into a JSON object, or None in case of an error.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from API: {e}")
            return None

    @staticmethod
    def write_data_to_file(data: List[Dict[str, Any]], data_class: str) -> None:
        """
        Writes the provided data into a JSON file.

        Args:
            data (List[Dict]): The data to be written to the file.
            data_class (str): The type of data to be written to the file.
        """
        if data_class == "pokemon":
            with open("data/pokemon.json", "w", encoding="utf-8") as file:
                json.dump(data, file, indent=4)

        elif data_class == "type":
            with open("data/type_class.json", "w", encoding="utf-8") as file:
                json.dump(data, file, indent=4)

    def fetch_detailed_data(self, urls: List[str]) -> List[Dict[str, Any]]:
        """
        Fetches detailed data for each Pokémon in the provided list of URLs.

        Args:
            urls (List[str]): The list of URLs for the Pokémon.

        Returns:
            List[Dict]: The detailed data for each Pokémon.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            return list(executor.map(self.api_call, urls))

    @staticmethod
    def transform_data(
        data: List[Dict[str, Any]], data_class: str
    ) -> List[Dict[str, Any]]:
        """
        Transforms the provided data into the required format.

        Args:
            data (List[Dict]): The data to be transformed.
            data_class (str): The type of data to be transformed.

        Returns:
            List[Dict]: The transformed data.
        """
        if data_class == "pokemon":
            return [
                {
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
                for pokemon in data
            ]

        elif data_class == "type":
            return [
                {
                    "id": type_class["id"],
                    "name": type_class["name"],
                    "damage_relations": {
                        "double_damage_from": [
                            item["name"]
                            for item in type_class["damage_relations"][
                                "double_damage_from"
                            ]
                        ],
                        "double_damage_to": [
                            item["name"]
                            for item in type_class["damage_relations"][
                                "double_damage_to"
                            ]
                        ],
                        "half_damage_from": [
                            item["name"]
                            for item in type_class["damage_relations"][
                                "half_damage_from"
                            ]
                        ],
                        "half_damage_to": [
                            item["name"]
                            for item in type_class["damage_relations"]["half_damage_to"]
                        ],
                        "no_damage_from": [
                            item["name"]
                            for item in type_class["damage_relations"]["no_damage_from"]
                        ],
                        "no_damage_to": [
                            item["name"]
                            for item in type_class["damage_relations"]["no_damage_to"]
                        ],
                    },
                }
                for type_class in data
            ]

    @staticmethod
    def log_metadata(start_time, end_time) -> None:
        """
        Logs the metadata for the extraction process.

        Args:
            start_time (float): The time at which the extraction process started.
            end_time (float): The time at which the extraction process ended.
        """
        logging.info(
            f"Extraction process started at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
        )
        logging.info(
            f"Extraction process ended at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}"
        )
        logging.info(
            f"Extraction process time taken: {end_time - start_time} seconds\n"
        )

    def extract_pokemon(self) -> None:
        """
        Extracts Pokémon data from the PokéAPI, transforms the data, and writes it to a JSON file.

        The extraction is done in a paginated way, where each page contains multiple Pokémon.
        For each Pokémon, detailed data is fetched asynchronously using a ThreadPoolExecutor for improved speed.
        The extracted data for each Pokémon includes its ID, order, name, stats, types, height, weight, and species.

        The entire extraction process, including the time it took, is logged.
        """
        url = self.pokemon_url
        results = []

        start_time = time.time()

        while url:
            data = self.api_call(url)

            if data is None:
                logging.error("Error fetching data from API")
                return

            pokemon_urls = [pokemon["url"] for pokemon in data["results"]]
            detailed_data = self.fetch_detailed_data(pokemon_urls)
            transformed_data = self.transform_data(detailed_data, "pokemon")
            logging.info(transformed_data)
            results.extend(transformed_data)

            url = data["next"]

        end_time = time.time()

        self.log_metadata(start_time, end_time)

        self.write_data_to_file(results)

    def extract_types(self) -> None:
        """
        Extracts type data from the PokéAPI and writes it to a JSON file.
        """
        data = self.api_call(self.type_url)

        if data is None:
            logging.error("Error fetching data from API")
            return

        results = data["results"]

        start_time = time.time()

        type_urls = [type["url"] for type in results]

        detailed_data = self.fetch_detailed_data(type_urls)
        transformed_data = self.transform_data(detailed_data, "type")
        logging.info(transformed_data)

        end_time = time.time()

        self.log_metadata(start_time, end_time)

        self.write_data_to_file(transformed_data, "type")


obj = Extraction()
# obj.extract_pokemon()
obj.extract_types()
