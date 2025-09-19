import requests
import csv
import re
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import time

class CoralIslandScraper:
    def __init__(self):
        self.base_url = "https://coral.guide"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Tradução dos nomes dos itens para português brasileiro
        self.name_translations = {
            'peony': 'Peônia',
            'daisy': 'Margarida',
            'turnip': 'Nabo',
            'potato': 'Batata',
            'cauliflower': 'Couve-flor',
            'carrot': 'Cenoura',
            'poppy': 'Papoula',
            'banana': 'Banana',
            'plum': 'Ameixa',
            'rambutan': 'Rambutan',
            'strawberry': 'Morango',
            'radish': 'Rabanete',
            'peas': 'Ervilha',
            'sugarcane': 'Cana-de-açúcar',
            'cucumber': 'Pepino',
            'lettuce': 'Alface',
            'soybean': 'Soja',
            'snowdrop': 'Campainha-branca',
            'durian': 'Durian',
            'orange': 'Laranja',
            'lychee': 'Lichia',
            'blueberry': 'Mirtilo',
            'hot_pepper': 'Pimenta',
            'corn': 'Milho',
            'melon': 'Melão',
            'tomato': 'Tomate',
            'red_cabbage': 'Repolho Roxo',
            'wheat': 'Trigo',
            'bell_pepper': 'Pimentão',
            'pineapple': 'Abacaxi',
            'okra': 'Quiabo',
            'blackberry': 'Amora',
            'coffee_beans': 'Grãos de Café',
            'starfruit': 'Carambola',
            'gardenia': 'Gardênia',
            'sunflower': 'Girassol',
            'rose': 'Rosa',
            'lily': 'Lírio',
            'iris': 'Íris',
            'mango': 'Manga',
            'peach': 'Pêssego',
            'jackfruit': 'Jaca',
            'dragonfruit': 'Pitaya',
            'papaya': 'Mamão',
            'rice': 'Arroz',
            'barley': 'Cevada',
            'basil': 'Manjericão',
            'amaranth': 'Amaranto',
            'artichoke': 'Alcachofra',
            'beet': 'Beterraba',
            'grape': 'Uva',
            'bok_choy': 'Bok Choy',
            'cranberry': 'Oxicoco',
            'eggplant': 'Berinjela',
            'pumpkin': 'Abóbora',
            'taro_root': 'Raiz de Taro',
            'sweet_potato': 'Batata-doce',
            'blue_dahlia': 'Dália Azul',
            'cactus': 'Cacto',
            'orchid': 'Orquídea',
            'apple': 'Maçã',
            'olive': 'Azeitona',
            'cocoa_bean': 'Grão de Cacau',
            'lemon': 'Limão',
            'pear': 'Pera',
            'garlic': 'Alho',
            'cotton': 'Algodão',
            'tea_leaf': 'Folha de Chá',
            'almond': 'Amêndoa',
            'avocado': 'Abacate',
            'snake_fruit': 'Salak',
            'fairy_rose': 'Rosa Fada',
            'chard': 'Acelga',
            'watermelon': 'Melancia'
        }
        
        # Mapping de seasons baseado nos nomes dos arquivos de sprite
        self.season_mapping = {
            'peony': 'Spring',
            'daisy': 'Spring', 
            'turnip': 'Spring',
            'potato': 'Spring',
            'cauliflower': 'Spring',
            'carrot': 'Spring',
            'poppy': 'Spring',
            'banana': 'Spring',
            'plum': 'Spring',
            'rambutan': 'Spring',
            'strawberry': 'Spring',
            'radish': 'Spring/Summer',
            'peas': 'Spring',
            'sugarcane': 'Spring/Summer/Fall',
            'cucumber': 'Summer',
            'lettuce': 'Summer',
            'soybean': 'Summer',
            'snowdrop': 'Winter',
            'durian': 'Summer',
            'orange': 'Summer',
            'lychee': 'Summer',
            'blueberry': 'Summer',
            'hot_pepper': 'Summer/Fall',
            'corn': 'Summer',
            'melon': 'Summer',
            'tomato': 'Summer',
            'red_cabbage': 'Summer',
            'wheat': 'Summer',
            'bell_pepper': 'Summer',
            'pineapple': 'Summer',
            'okra': 'Summer',
            'blackberry': 'Summer',
            'coffee_beans': 'Summer/Fall',
            'starfruit': 'Summer',
            'gardenia': 'Summer',
            'sunflower': 'Summer/Fall',
            'rose': 'Summer/Fall',
            'lily': 'Summer',
            'iris': 'Summer',
            'mango': 'Summer',
            'peach': 'Summer',
            'jackfruit': 'Summer',
            'dragonfruit': 'Summer',
            'papaya': 'Summer',
            'rice': 'Summer',
            'barley': 'Fall',
            'basil': 'Fall',
            'amaranth': 'Fall',
            'artichoke': 'Fall',
            'beet': 'Fall',
            'grape': 'Fall',
            'bok_choy': 'Fall',
            'cranberry': 'Fall',
            'eggplant': 'Fall',
            'pumpkin': 'Fall',
            'taro_root': 'Fall',
            'sweet_potato': 'Fall',
            'blue_dahlia': 'Fall',
            'cactus': 'Fall',
            'orchid': 'Fall',
            'apple': 'Fall',
            'olive': 'Fall',
            'cocoa_bean': 'Fall',
            'lemon': 'Fall',
            'pear': 'Fall',
            'garlic': 'Fall',
            'cotton': 'Fall',
            'tea_leaf': 'Fall',
            'almond': 'Fall',
            'avocado': 'Fall',
            'snake_fruit': 'Fall',
            'fairy_rose': 'Fall',
            'chard': 'Fall',
            'watermelon': 'Summer'
        }

    def extract_item_name(self, filename):
        """Extrai o nome do item do filename do sprite e traduz para português"""
        # Remove extensão e sufixo _Sprite
        name = filename.replace('.webp', '').replace('_Sprite', '')
        
        # Converte para lowercase para busca na tradução
        search_key = name.lower()
        
        # Retorna tradução em português ou nome original capitalizado
        if search_key in self.name_translations:
            return self.name_translations[search_key]
        else:
            # Fallback: substitui underscores por espaços e capitaliza
            return name.replace('_', ' ').title()

    def get_season_from_filename(self, filename):
        """Obtém a season baseada no nome do arquivo original (em inglês)"""
        # Remove extensão e sufixo _Sprite para obter o nome em inglês
        name = filename.replace('.webp', '').replace('_Sprite', '')
        
        # Remove underscore extra no final se existir
        if name.endswith('_'):
            name = name[:-1]
            
        search_key = name.lower()
        
        # Debug - vamos ver o que está acontecendo
        season = self.season_mapping.get(search_key, 'Unknown')
        if season == 'Unknown':
            print(f"Debug: Não encontrado mapping para '{search_key}' (filename: {filename})")
        
        return season

    def scrape_page(self, url, category):
        """Scrapa uma página específica"""
        try:
            print(f"Fazendo scraping de: {url}")
            response = self.session.get(url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            items = []
            
            # Primeiro, vamos tentar encontrar informações estruturadas sobre seasons
            self.extract_season_info_from_page(soup)
            
            # Busca por todas as imagens na página
            images = soup.find_all('img')
            
            for img in images:
                src = img.get('src', '')
                
                # Verifica se é uma imagem de sprite válida
                if 'assets/live/items/icons' in src and '_Sprite' in src:
                    # Constrói URL completa
                    full_url = urljoin(self.base_url, src)
                    
                    # Extrai nome do arquivo
                    filename = src.split('/')[-1].split('?')[0]  # Remove query parameters
                    
                    # Extrai nome do item
                    item_name = self.extract_item_name(filename)
                    
                    # Tenta extrair season do contexto da imagem
                    season = self.extract_season_from_context(img, filename)
                    
                    items.append({
                        'url': full_url,
                        'name': item_name,
                        'season': season,
                        'category': category
                    })
            
            print(f"Encontrados {len(items)} itens em {category}")
            return items
            
        except Exception as e:
            print(f"Erro ao fazer scraping de {url}: {str(e)}")
            return []

    def extract_season_info_from_page(self, soup):
        """Tenta extrair informações de season da estrutura da página"""
        # Busca por elementos que possam conter informações de season
        season_indicators = soup.find_all(string=lambda text: text and any(season in text.lower() for season in ['spring', 'summer', 'fall', 'winter']))
        
        if season_indicators:
            print(f"Debug: Encontrados {len(season_indicators)} indicadores de season na página")
            for indicator in season_indicators[:3]:  # Mostra apenas os primeiros 3
                print(f"Debug: Season indicator: {indicator.strip()}")

    def extract_season_from_context(self, img, filename):
        """Tenta extrair season do contexto da imagem (elementos próximos)"""
        # Primeiro tenta o mapeamento estático
        season = self.get_season_from_filename(filename)
        
        if season != 'Unknown':
            return season
            
        # Se não encontrou no mapeamento, tenta extrair do contexto HTML
        parent = img.parent
        while parent and parent.name not in ['html', 'body']:
            text = parent.get_text().lower()
            for season_name in ['spring', 'summer', 'fall', 'winter']:
                if season_name in text:
                    return season_name.capitalize()
            parent = parent.parent
            
        return 'Unknown'

    def scrape_all_categories(self):
        """Scrapa todas as categorias"""
        categories = {
            'crops': 'https://coral.guide/journal/produce/crops',
            'animal_products': 'https://coral.guide/journal/produce/animalproducts',
            'artisan_products': 'https://coral.guide/journal/produce/artisanproducts',
            'ocean': 'https://coral.guide/journal/produce/ocean'
        }
        
        all_items = []
        
        for category, url in categories.items():
            items = self.scrape_page(url, category)
            all_items.extend(items)
            time.sleep(1)  # Pausa entre requests para ser respeitoso
        
        return all_items

    def save_to_csv(self, items, filename='coral_island.csv'):
        """Salva os itens em um arquivo CSV"""
        if not items:
            print("Nenhum item encontrado para salvar.")
            return
            
        print(f"Salvando {len(items)} itens em {filename}")
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['url', 'name', 'season', 'category']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for item in items:
                writer.writerow(item)
        
        print(f"Arquivo CSV salvo como: {filename}")

def main():
    scraper = CoralIslandScraper()
    
    print("Iniciando coleta de dados do Coral Island...")
    print("=" * 50)
    
    # Scrapa todas as categorias
    all_items = scraper.scrape_all_categories()
    
    if all_items:
        # Salva em CSV
        scraper.save_to_csv(all_items)
        
        # Mostra estatísticas
        print("\n" + "=" * 50)
        print("RESUMO DA COLETA:")
        print("=" * 50)
        
        categories = {}
        for item in all_items:
            category = item['category']
            if category not in categories:
                categories[category] = 0
            categories[category] += 1
        
        for category, count in categories.items():
            print(f"{category.capitalize()}: {count} itens")
        
        print(f"\nTotal: {len(all_items)} itens coletados")
        
        # Mostra alguns exemplos
        print("\nPrimeiros 5 itens:")
        for i, item in enumerate(all_items[:5]):
            print(f"{i+1}. {item['name']} - {item['season']} ({item['category']})")
        
        # Verifica se Peônia e Melancia foram encontrados
        peony_found = any(item['name'] == 'Peony' for item in all_items)
        watermelon_found = any(item['name'] == 'Watermelon' for item in all_items)
        
        print(f"\nPeônia encontrada: {'✓' if peony_found else '✗'}")
        print(f"Melancia encontrada: {'✓' if watermelon_found else '✗'}")
        
    else:
        print("Nenhum item foi coletado. Verifique a conexão e tente novamente.")

if __name__ == "__main__":
    main()