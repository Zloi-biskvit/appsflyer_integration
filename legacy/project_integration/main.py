from datetime import datetime

from src.config import load_config
from src.appsflyer_client import AppsFlyerClient
from src.integration_service import IntegrationService

# TODO  //fatel69967@gusronk.com qwe1qwe metabase
def main() -> None:
    cfg = load_config()

    # current_date что бы руками каждый раз новый день не прописывать
    if False:
        today_str = datetime.today().strftime("%d-%m-%y")
        cfg.to_date = today_str
        print("current_day on :", today_str)

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
