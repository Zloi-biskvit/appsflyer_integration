# main.py
from config import load_config
from appsflyer_client import AppsFlyerClient
from integration_service import IntegrationService


def main() -> None:
    cfg = load_config()

    # 2. инициализируем клиента AppsFlyer
    client = AppsFlyerClient(
        api_token=cfg.api_token,
        from_date=cfg.from_date,
        to_date=cfg.to_date,
        timezone=cfg.timezone,
        retargeting=cfg.retargeting,
    )

    # 3. создаём сервис, который склеивает клиента и БД
    service = IntegrationService(config=cfg, client=client)

    # 4. запускаем сценарий
    service.run()


if __name__ == "__main__":
    main()
