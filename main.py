from extraction import Extraction


def main():
    ext_obj = Extraction()
    ext_obj.extract_data("item")
    # list(map(ext_obj.extract_data, ["pokemon", "type", "move", "ability", "item"]))


if __name__ == "__main__":
    main()
