import requests
import json
import concurrent.futures
import time
import re
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
        self.endpoints = {
            "pokemon": self.base_url + "pokemon-species/",
            "type": self.base_url + "type/",
            "move": self.base_url + "move/",
            "ability": self.base_url + "ability/",
            "item": self.base_url + "item/",
        }

    @staticmethod
    def api_call(url: str) -> Optional[Dict[str, Any]]:
        """
        Makes a GET request to the specified API URL and returns the response as a JSON object.
        In case of an HTTP error, it logs the exception and returns None. Possible reasons for None return value
        include connection errors, timeout errors, and HTTP errors.

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
            # Raises an error in the log file if the GET request fails
            logging.error(f"Error fetching data from API: {e}")
            return None

    @staticmethod
    def write_data_to_file(data: List[Dict[str, Any]], data_class: str) -> None:
        """
        Writes the provided data into a JSON file. The file will be created in the 'data' directory.
        If the 'data' directory does not exist, it will cause a FileNotFoundError.

        Args:
            data (List[Dict]): The data to be written to the file.
            data_class (str): The type of data to be written to the file.
        """
        with open(f"data/{data_class}.json", "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)

    def fetch_detailed_data(self, urls: List[str]) -> List[Optional[Dict[str, Any]]]:
        """
        Fetches detailed data for each Pokémon, type, or move in the provided list of URLs.
        It uses ThreadPoolExecutor to concurrently send multiple GET requests to improve the speed of data fetching.

        Args:
            urls (List[str]): The list of URLs for the Pokémon, type, or move.

        Returns:
            List[Optional[Dict]]: The detailed data for each Pokémon, type, or move.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            return list(executor.map(self.api_call, urls))

    @staticmethod
    def transform_data(
        data: List[Optional[Dict[str, Any]]], data_class: str
    ) -> List[Dict[str, Any]]:
        """
        Transforms the provided data into the required format. Depending on the data_class,
        different transformations are applied.
        For 'pokemon', ID, order, name, stats, types, height, weight, and species is extracted.
        For 'move', ID, name, power, pp, type, and class is extracted.
        For 'type', data like ID, name, and damage relations is extracted.
        For 'ability', ID, name, and effect is extracted.
        For 'item', ID, name, and effect is extracted.

        Args:
            data (List[Optional[Dict]]): The data to be transformed.
            data_class (str): The type of data to be transformed.
            Can be "pokemon", "type", "move", "ability" or "item".

        Returns:
            List[Dict]: The transformed data.
        """
        if data_class == "pokemon":
            return [
                {
                    "id": pokemon.get("id"),
                    "order": pokemon.get("order"),
                    "name": pokemon.get("forms", [{}])[0].get("name"),
                    "stats": {
                        stat.get("stat", {}).get("name"): stat.get("base_stat")
                        for stat in pokemon.get("stats", [])
                    },
                    "types": [
                        item.get("type", {}).get("name")
                        for item in pokemon.get("types", [])
                    ],
                    "moves": [
                        item["move"]["name"] for item in pokemon.get("moves", [])
                    ],
                    "height": pokemon.get("height", 0) / 10,  # Decimeters to meters
                    "weight": pokemon.get("weight", 0) / 10,  # Decagrams to kilograms
                    "species": pokemon.get("species", {}).get("name"),
                    "game_indices": [
                        item["version"]["name"]
                        for item in pokemon.get("game_indices", [])
                    ],
                }
                for pokemon in data
                if pokemon is not None
            ]

        elif data_class == "move":
            return [
                {
                    "id": move.get("id"),
                    "name": move.get("name"),
                    "power": move.get("power"),
                    "pp": move.get("pp"),
                    "type": move.get("type", {}).get("name"),
                    "class": move.get("damage_class", {}).get("name"),
                }
                for move in data
                if move is not None
            ]

        elif data_class == "type":
            return [
                {
                    "id": type_class.get("id"),
                    "name": type_class.get("name"),
                    "damage_relations": {
                        "double_damage_from": [
                            item.get("name")
                            for item in type_class.get("damage_relations", {}).get(
                                "double_damage_from", []
                            )
                        ],
                        "double_damage_to": [
                            item.get("name")
                            for item in type_class.get("damage_relations", {}).get(
                                "double_damage_to", []
                            )
                        ],
                        "half_damage_from": [
                            item.get("name")
                            for item in type_class.get("damage_relations", {}).get(
                                "half_damage_from", []
                            )
                        ],
                        "half_damage_to": [
                            item.get("name")
                            for item in type_class.get("damage_relations", {}).get(
                                "half_damage_to", []
                            )
                        ],
                        "no_damage_from": [
                            item.get("name")
                            for item in type_class.get("damage_relations", {}).get(
                                "no_damage_from", []
                            )
                        ],
                        "no_damage_to": [
                            item.get("name")
                            for item in type_class.get("damage_relations", {}).get(
                                "no_damage_to", []
                            )
                        ],
                    },
                }
                for type_class in data
                if type_class is not None
            ]

        elif data_class == "ability":
            return [
                {
                    "id": ability.get("id"),
                    "name": ability.get("name"),
                    "pokemon": [
                        item["pokemon"]["name"] for item in ability.get("pokemon", [])
                    ],
                }
                for ability in data
            ]

        elif data_class == "item":
            return [
                {
                    "id": item.get("id"),
                    "name": item.get("name"),
                    "category": item.get("category", {}).get("name", []),
                }
                for item in data
            ]

    @staticmethod
    def log_metadata(
        start_time: float, end_time: float, data_class: str, count: int, api_count: int
    ) -> None:
        """
        Logs the metadata for the extraction process. The metadata includes the start and end times,
        total time taken, total number of data items extracted, and the expected number of items.

        Args:
            start_time (float): The time at which the extraction process started.
            end_time (float): The time at which the extraction process ended.
            data_class (str): The type of data that was extracted. Can be "pokemon", "type", or "move".
            count (int): Total number of data items that were successfully extracted.
            api_count (int): Expected number of items as per the API response.
        """
        logging.info(
            f"Extraction process started at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
        )
        logging.info(
            f"Extraction process ended at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}"
        )
        logging.info(f"Extraction process time taken: {end_time - start_time} seconds")
        logging.info(
            f"Total number of {data_class.capitalize()} extracted: {count} (Expected: {api_count})\n"
        )

    def extract_data(self, data_class: str) -> None:
        """
        Extracts data for the specified class from the PokéAPI, transforms the data, and writes it to a JSON file.
        The data class can be one of "pokemon", "type", or "move".

        The extraction is done in a paginated way, where each page contains multiple data items.
        For each item, detailed data is fetched asynchronously using a ThreadPoolExecutor for improved speed.
        The extracted data for each class will differ based on the `data_class`.

        If an error occurs during data fetching from the API, the function logs the error and returns immediately.

        The entire extraction process, including the time it took, is logged.

        Args:
            data_class (str): The type of data to be extracted.
            Can be "pokemon", "type", "move", "ability" or "item".
        """
        url = self.endpoints[data_class]
        results = []
        count = 0

        start_time = time.time()

        while url is not None:
            data = self.api_call(url)

            if data is None:
                logging.error("Error fetching data from API")
                return

            if data_class == "pokemon":
                data_urls = [
                    re.sub("-species", "", data_item["url"])
                    for data_item in data["results"]
                ]
            else:
                data_urls = [data_item["url"] for data_item in data["results"]]

            detailed_data = self.fetch_detailed_data(data_urls)
            transformed_data = self.transform_data(detailed_data, data_class)
            logging.debug(transformed_data)
            results.extend(transformed_data)

            count += len(transformed_data)

            url = data["next"]

        end_time = time.time()

        api_count = data["count"]

        self.log_metadata(start_time, end_time, data_class, count, api_count)

        self.write_data_to_file(results, data_class)
