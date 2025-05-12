import core.config_loader as config_loader

if __name__ == "__main__":
    config = config_loader.load_config("doris_db.host")
    print(config)
