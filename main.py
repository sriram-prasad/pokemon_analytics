from extraction import Extraction


def main():
    ext_obj = Extraction()
    list(map(ext_obj.extract_data, ["pokemon", "type", "move", "ability"]))


if __name__ == "__main__":
    main()
