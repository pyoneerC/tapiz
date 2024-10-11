import requests
from lxml import html
import pandas as pd
from datetime import datetime

base_url = "https://www.espaciovino.com.ar/vinos"

def scrape_wine_data():
    response = requests.get(base_url)
    response.raise_for_status()
    page_content = html.fromstring(response.content)

    wines = []
    wine_elements = page_content.xpath("//div[@class='product']")

    for wine in wine_elements[:5]:  # Scraping only the first 5 wines
        wine_name = wine.xpath(".//div[@class='data']//div[@class='name']//h2//a/text()")[0].strip()

        # Get the wine's subpage link
        wine_link = wine.xpath(".//div[@class='data']//div[@class='name']//h2//a/@href")[0].strip()
        full_wine_link = f"https://www.espaciovino.com.ar{wine_link}"

        # Extract original price
        original_price_tag = wine.xpath(".//span[@class='product-list-price']/text()")
        original_price = original_price_tag[0].strip() if original_price_tag else "N/A"
        original_price_cleaned = original_price[:-3].replace('$', '').replace('.', '').replace(',', '')

        # Extract current price
        fraction_tag = wine.xpath(".//span[@class='product-price-fraction']/text()")
        decimal_tag = wine.xpath(".//span[@class='product-price-decimal']/text()")

        if fraction_tag and decimal_tag:
            current_price = f"${fraction_tag[0].strip()},{decimal_tag[0].strip()}"
            current_price_cleaned = current_price[:-3].replace('$', '').replace('.', '').replace(',', '')
        else:
            current_price = "N/A"
            current_price_cleaned = None

        # Calculate percentage difference if prices are valid
        if original_price_cleaned and current_price_cleaned:
            original_price_float = float(original_price_cleaned)
            current_price_float = float(current_price_cleaned)
            percentage_difference = ((original_price_float - current_price_float) / original_price_float) * 100
        else:
            percentage_difference = None

        # Scrape additional wine details from the subpage
        producer, variety, blend, wine_type = scrape_additional_wine_info(full_wine_link)

        wines.append({
            "Nombre del Producto": wine_name,
            "Precio Original": original_price[:-3],
            "Precio Descontado": current_price[:-3],
            "Descuento (%)": f"{percentage_difference:.0f}%" if percentage_difference is not None else "N/A",
            "Productor": producer,
            "Variedad": variety,
            "Corte": blend,
            "Tipo": wine_type,  # Added wine type to the output
        })

    return wines

def scrape_additional_wine_info(wine_url):
    """Scrapes producer, variety, blend, and type from the wine subpage."""
    response = requests.get(wine_url)
    response.raise_for_status()
    page_content = html.fromstring(response.content)

    # Scraping producer
    producer = page_content.xpath("//div[@class='value']//a/span[@itemprop='name']/text()")
    producer = producer[0].strip() if producer else "N/A"

    # Scraping variety
    variety = page_content.xpath("//div[contains(text(), 'VARIEDAD')]/following-sibling::div[@class='value']/a/text()")
    variety = ', '.join([v.strip() for v in variety]) if variety else "N/A"

    # Scraping blend
    blend = page_content.xpath("//div[contains(text(), 'CORTE')]/following-sibling::div[@class='value']/text()")
    blend = blend[0].strip() if blend else "N/A"

    # Scraping type - updated to grab the text from the sibling div
    wine_type = page_content.xpath("//div[contains(text(), 'TIPO')]/following-sibling::div[@class='value']/a/text()")
    wine_type = wine_type[0].strip() if wine_type else "N/A"

    return producer, variety, blend, wine_type  # Returning wine type

def main():
    wine_data = scrape_wine_data()
    df = pd.DataFrame(wine_data)
    current_month_year = datetime.now().strftime("%b %Y")
    excel_filename = f"PRECIOS COMPETENCIA {current_month_year}.xlsx"
    df.to_excel(excel_filename, index=False)
    print(f"Data saved to {excel_filename}")

if __name__ == "__main__":
    main()
