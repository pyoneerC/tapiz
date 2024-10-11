import asyncio
import aiohttp
from lxml import html
import pandas as pd
from datetime import datetime

base_url = "https://www.espaciovino.com.ar/vinos"
pages = 2 # Each page takes approximately 20 seconds to scrape, max pages is 92.
max_concurrent_requests = 1000

async def fetch(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()

async def scrape_wine_data(page_limit):
    wines = []

    semaphore = asyncio.Semaphore(max_concurrent_requests)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(1, page_limit + 1):
            print(f"Preparing to scrape page {i} of {page_limit}...")
            tasks.append(fetch(session, f"{base_url}?page={i}"))

        page_contents = await asyncio.gather(*tasks)

        wine_tasks = []
        for page_content in page_contents:
            page_tree = html.fromstring(page_content)
            wine_elements = page_tree.xpath("//div[@class='product']")

            for wine in wine_elements:
                wine_name = wine.xpath(".//div[@class='data']//div[@class='name']//h2//a/text()")[0].strip()
                wine_link = wine.xpath(".//div[@class='data']//div[@class='name']//h2//a/@href")[0].strip()
                full_wine_link = f"https://www.espaciovino.com.ar{wine_link}"

                original_price_tag = wine.xpath(".//span[@class='product-list-price']/text()")
                original_price = original_price_tag[0].strip() if original_price_tag else "N/A"
                original_price_cleaned = original_price[:-3].replace('$', '').replace('.', '').replace(',', '')

                fraction_tag = wine.xpath(".//span[@class='product-price-fraction']/text()")
                decimal_tag = wine.xpath(".//span[@class='product-price-decimal']/text()")

                if fraction_tag and decimal_tag:
                    current_price = f"${fraction_tag[0].strip()},{decimal_tag[0].strip()}"
                    current_price_cleaned = current_price[:-3].replace('$', '').replace('.', '').replace(',', '')
                else:
                    current_price = "N/A"
                    current_price_cleaned = None

                if original_price_cleaned and current_price_cleaned:
                    original_price_float = float(original_price_cleaned)
                    current_price_float = float(current_price_cleaned)
                    percentage_difference = ((original_price_float - current_price_float) / original_price_float) * 100
                else:
                    percentage_difference = None

                wine_tasks.append(scrape_additional_wine_info(semaphore, session, full_wine_link, wine_name, original_price, current_price, percentage_difference))

        wines.extend(await asyncio.gather(*wine_tasks))

    return wines

async def scrape_additional_wine_info(semaphore, session, wine_url, wine_name, original_price, current_price, percentage_difference):
    async with semaphore:
        page_content = await fetch(session, wine_url)
        page_tree = html.fromstring(page_content)

        producer = page_tree.xpath("//div[@class='value']//a/span[@itemprop='name']/text()")
        producer = producer[0].strip() if producer else "N/A"

        variety = page_tree.xpath("//div[contains(text(), 'VARIEDAD')]/following-sibling::div[@class='value']/a/text()")
        variety = ', '.join([v.strip() for v in variety]) if variety else "N/A"

        blend = page_tree.xpath("//div[contains(text(), 'CORTE')]/following-sibling::div[@class='value']/text()")
        blend = blend[0].strip() if blend else "N/A"

        wine_type = page_tree.xpath("//div[contains(text(), 'TIPO')]/following-sibling::div[@class='value']/a/text()")
        wine_type = wine_type[0].strip() if wine_type else "N/A"

        return {
            "Nombre del Producto": wine_name,
            "Productor": producer,
            "Variedad": variety,
            "Corte": blend,
            "Tipo": wine_type,
            "Precio Original": original_price[:-3],
            "Precio Descontado": current_price[:-3],
            "Descuento (%)": f"{percentage_difference:.0f}%" if percentage_difference is not None else "",
        }

def main():
    wine_data = asyncio.run(scrape_wine_data(pages))
    df = pd.DataFrame(wine_data)
    current_month_year = datetime.now().strftime("%b %Y")
    excel_filename = f"PRECIOS COMPETENCIA {current_month_year}.xlsx"
    df.to_excel(excel_filename, index=False)
    print(f"Data saved to {excel_filename}")

if __name__ == "__main__":
    main()
