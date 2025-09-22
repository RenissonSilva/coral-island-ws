import csv
import json
import re
import time
from dataclasses import dataclass, asdict
from typing import List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup, Tag
try:
    from playwright.sync_api import sync_playwright, Browser, Page
    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PLAYWRIGHT_AVAILABLE = False


BASE_URLS = [
    "https://coral.guide/journal/produce/crops",
    # "https://coral.guide/journal/produce/animalproducts",
    # "https://coral.guide/journal/produce/artisanproducts",
    # "https://coral.guide/journal/produce/ocean",
    # "https://coral.guide/journal/caught/fish",
    # "https://coral.guide/journal/caught/insects",
    # "https://coral.guide/journal/caught/seacritters",
    # "https://coral.guide/journal/found/artifacts",
    # "https://coral.guide/journal/found/gems",
    # "https://coral.guide/journal/found/fossils",
    # "https://coral.guide/journal/found/scavangables",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8",
}

SEASON_WORDS = ["spring", "summer", "fall", "autumn", "winter", "all seasons", "year round"]
WEATHER_WORDS = ["sunny", "rainy", "stormy", "windy", "snowy", "cloudy"]

# Fandom sources (páginas estáticas)
FANDOM_URLS = [
    "https://coralisland.fandom.com/wiki/Crop",  # crops
    "https://coralisland.fandom.com/wiki/Animal_Products",  # animal products
    "https://coralisland.fandom.com/wiki/Artisan_Goods",  # artisan products
    "https://coralisland.fandom.com/wiki/Ocean_Farming",  # ocean crops
    "https://coralisland.fandom.com/wiki/Fish",  # fish
    "https://coralisland.fandom.com/wiki/Insects",  # insects
    "https://coralisland.fandom.com/wiki/Sea_Critters",  # sea critters
    "https://coralisland.fandom.com/wiki/Artifacts",  # artifacts
    "https://coralisland.fandom.com/wiki/Gems",  # gems
    "https://coralisand.fandom.com/wiki/Fossil".replace("coralisand", "coralisland"),  # fossils
    "https://coralisland.fandom.com/wiki/Scavengeables",  # scavangables
]


@dataclass
class ItemRow:
    image: str
    name: str
    seasons: str
    weather: str
    source_page: str
    category: str = ""


def get_soup(url: str, retries: int = 3, backoff: float = 1.5) -> BeautifulSoup:
    last_exc: Optional[Exception] = None
    for i in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 200:
                return BeautifulSoup(resp.text, "lxml")
            else:
                last_exc = RuntimeError(f"HTTP {resp.status_code} for {url}")
        except Exception as e:
            last_exc = e
        time.sleep(backoff * (i + 1))
    raise last_exc  # type: ignore


class BrowserSession:
    def __init__(self, headless: bool = True):
        self._pw = None
        self._browser: Optional[Browser] = None
        self._headless = headless

    def __enter__(self):
        if not PLAYWRIGHT_AVAILABLE:
            return None
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=self._headless)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._browser:
                self._browser.close()
        finally:
            if self._pw:
                self._pw.stop()

    def soup(self, url: str, wait_selector: Optional[str] = None) -> BeautifulSoup:
        assert self._browser is not None
        page: Page = self._browser.new_page(user_agent=HEADERS["User-Agent"])
        page.set_default_timeout(30000)
        page.goto(url, wait_until="domcontentloaded")
        if wait_selector:
            try:
                page.wait_for_selector(wait_selector, state="attached", timeout=10000)
            except Exception:
                pass
        # auto-scroll para carregar itens lazy
        try:
            page.evaluate(
                """
                async () => {
                  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
                  let last = 0; let same = 0;
                  for (let i=0; i<20; i++) {
                    window.scrollTo(0, document.body.scrollHeight);
                    await sleep(500);
                    const h = document.body.scrollHeight;
                    if (h === last) { same++; } else { same = 0; }
                    last = h;
                    if (same >= 3) break;
                  }
                }
                """
            )
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        html = page.content()
        page.close()
        return BeautifulSoup(html, "lxml")

    def extract_items_from_journal(self, url: str) -> List[Tuple[str, str, str]]:
        # Retorna lista de (name, image_src, abs_href)
        assert self._browser is not None
        page: Page = self._browser.new_page(user_agent=HEADERS["User-Agent"])
        page.set_default_timeout(30000)
        page.goto(url, wait_until="domcontentloaded")
        # auto-scroll
        try:
            page.evaluate(
                """
                async () => {
                  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
                  let last = 0; let same = 0;
                  for (let i=0; i<20; i++) {
                    window.scrollTo(0, document.body.scrollHeight);
                    await sleep(400);
                    const h = document.body.scrollHeight;
                    if (h === last) { same++; } else { same = 0; }
                    last = h;
                    if (same >= 3) break;
                  }
                }
                """
            )
        except Exception:
            pass
        items: List[Tuple[str, str, str]] = []
        # Seleciona imagens de itens e resolve nome e link via JS para lidar com shadow DOM
        img_loc = page.locator("img[src*='assets/live/items/icons']")
        count = img_loc.count()
        for i in range(count):
            try:
                img = img_loc.nth(i)
                # Busca anchor mais próximo e um label de nome próximo
                data = img.evaluate(
                    """
                    (el) => {
                        function closestAnchor(node){
                          while (node){
                            if (node.tagName === 'A' && node.getAttribute('href')) return node;
                            node = node.parentElement || (node.getRootNode && node.getRootNode().host) || null;
                          }
                          return null;
                        }
                        const a = closestAnchor(el);
                        const href = a ? a.getAttribute('href') : '';
                        // tenta encontrar nome textual nas proximidades
                        let name = '';
                        const root = (a || el).closest('div,li,article,section') || (a || el).parentElement;
                        if (root){
                          const nameCand = root.querySelector('.name, h3, h4, h2, [title]');
                          if (nameCand) name = nameCand.textContent.trim() || nameCand.getAttribute('title') || '';
                        }
                        const src = el.getAttribute('src') || '';
                        return {href, name, src};
                    }
                    """
                )
                href = data.get('href') or ''
                name = text_norm(data.get('name') or '')
                src = data.get('src') or ''
                if href.startswith('/'):
                    href = f"https://coral.guide{href}"
                if name and href:
                    items.append((name, src, href))
            except Exception:
                continue
        page.close()
        # de-dup pelo href
        seen = set()
        unique: List[Tuple[str, str, str]] = []
        for name, src, href in items:
            if href in seen:
                continue
            seen.add(href)
            unique.append((name, src, href))
        return unique

    def capture_json_from_page(self, url: str, duration_ms: int = 8000) -> List[Tuple[str, dict]]:
        assert self._browser is not None
        page: Page = self._browser.new_page(user_agent=HEADERS["User-Agent"])
        page.set_default_timeout(30000)
        captured: List[Tuple[str, dict]] = []
        def handle_response(resp):
            try:
                ctype = resp.headers.get("content-type", "")
                if "application/json" in ctype:
                    data = resp.json()
                    captured.append((resp.url, data))
            except Exception:
                pass
        page.on("response", handle_response)
        page.goto(url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=duration_ms)
        except Exception:
            pass
        # salvar arquivos
        try:
            from pathlib import Path
            outdir = Path("debug_json")
            outdir.mkdir(exist_ok=True)
            for i, (u, data) in enumerate(captured):
                safe = re.sub(r"[^a-zA-Z0-9]+", "_", u)[:80]
                (outdir / f"resp_{i:03d}_{safe}.json").write_text(
                    __import__("json").dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            print(f"  [debug] JSON capturado: {len(captured)} respostas salvas em debug_json/")
        except Exception:
            pass
        page.close()
        return captured


def text_norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def set_join(values: Set[str]) -> str:
    if not values:
        return ""
    # Keep a stable order: seasons in canonical order if possible, else alpha
    canon = ["Spring", "Summer", "Fall", "Winter", "All Seasons", "Year Round"]
    present = {v.lower(): v for v in values}
    ordered = [present.get(c.lower()) for c in canon if c.lower() in present]
    rest = sorted([v for k, v in present.items() if k not in {c.lower() for c in canon}])
    return "; ".join([v for v in ordered if v] + rest)


def extract_img_src(node: Tag) -> str:
    img = node.find("img")
    if img and img.get("src"):
        return img["src"]
    if img and img.get("data-src"):
        return img["data-src"]
    return ""


def find_by_labels(text: str, dictionary: List[str]) -> Set[str]:
    found: Set[str] = set()
    low = text.lower()
    for w in dictionary:
        if w in low:
            # Title case, special cases
            if w == "autumn":
                found.add("Fall")
            elif w == "all seasons":
                found.add("All Seasons")
            elif w == "year round":
                found.add("Year Round")
            else:
                found.add(w.title())
    return found


def parse_detail_page(url: str, browser: Optional[BrowserSession] = None) -> Tuple[Set[str], Set[str]]:
    seasons: Set[str] = set()
    weather: Set[str] = set()
    try:
        soup = browser.soup(url) if browser else get_soup(url)
    except Exception:
        return seasons, weather

    # Strategy 1: look for definition lists or info tables
    for table in soup.select("table, .infobox, .item-info, .details"):
        text = table.get_text(" ", strip=True)
        seasons |= find_by_labels(text, SEASON_WORDS)
        weather |= find_by_labels(text, WEATHER_WORDS)

    # Strategy 2: look for icon groups
    for grp in soup.select(".icons, .icon-list, .tags, .badges, .chips"):
        text = grp.get_text(" ", strip=True)
        seasons |= find_by_labels(text, SEASON_WORDS)
        weather |= find_by_labels(text, WEATHER_WORDS)

    # Strategy 3: any content section
    if not seasons or not weather:
        main = soup.find("main") or soup.find("article") or soup
        text = main.get_text(" ", strip=True)
        # Try to limit noise by cutting navigation repeats
        text = re.sub(r"\b(Coral Guide|Journal|Crafting|NPCs|Locations|Item database)\b", " ", text, flags=re.I)
        seasons |= find_by_labels(text, SEASON_WORDS)
        weather |= find_by_labels(text, WEATHER_WORDS)

    return seasons, weather


def parse_list_page(url: str, browser: Optional[BrowserSession] = None) -> List[ItemRow]:
    items: List[ItemRow] = []
    if browser:
        tuples = browser.extract_items_from_journal(url)
        for name, img_src, abs_href in tuples:
            det_seasons, det_weather = parse_detail_page(abs_href, browser=browser)
            if not img_src:
                try:
                    det_soup = browser.soup(abs_href)
                    header_img = det_soup.select_one("article img, main img, .header img, .infobox img")
                    if header_img and header_img.get("src"):
                        img_src = header_img["src"]
                except Exception:
                    pass
            items.append(
                ItemRow(
                    image=img_src,
                    name=name,
                    seasons=set_join(det_seasons),
                    weather=set_join(det_weather),
                    source_page=url,
                )
            )
        return items
    # fallback sem browser (pouco provável funcionar)
    soup = get_soup(url)
    for a in soup.select("a[href^='/database/']"):
        name = text_norm(a.get_text()) or text_norm(a.get("title", ""))
        if not name:
            continue
        href = a.get("href", "")
        abs_href = f"https://coral.guide{href}" if href.startswith("/") else href
        img_src = extract_img_src(a)
        det_seasons, det_weather = parse_detail_page(abs_href)
        items.append(ItemRow(image=img_src, name=name, seasons=set_join(det_seasons), weather=set_join(det_weather), source_page=url))
    
    # Deduplicate by name + source_page, keep first occurrence that has more info
    by_key = {}
    for it in items:
        key = (it.name.lower(), it.source_page)
        if key not in by_key:
            by_key[key] = it
        else:
            prev = by_key[key]
            # prefer entry with more fields filled
            prev_score = (len(prev.seasons) > 0) + (len(prev.weather) > 0) + (len(prev.image) > 0)
            new_score = (len(it.seasons) > 0) + (len(it.weather) > 0) + (len(it.image) > 0)
            if new_score > prev_score:
                by_key[key] = it
    return list(by_key.values())


def parse_database_index(browser: Optional[BrowserSession] = None) -> List[ItemRow]:
    url = "https://coral.guide/database"
    print("[fallback] Carregando índice do banco de dados de itens...")
    soup = browser.soup(url, wait_selector="a[href^='/database/']") if browser else get_soup(url)
    anchors = soup.select("main a[href^='/database/'], article a[href^='/database/'], a[href^='/database/']")
    items: List[ItemRow] = []
    seen: Set[str] = set()
    for a in anchors:
        href = a.get("href", "")
        if not href or href in seen:
            continue
        seen.add(href)
        abs_href = f"https://coral.guide{href}" if href.startswith("/") else href
        name = text_norm(a.get_text()) or text_norm(a.get("title", ""))
        if not name:
            parent = a.find_parent(["div", "li", "article", "section"]) or a
            tag = None
            for sel in [".name", "h3", "h4", "h2", "span"]:
                tag = parent.select_one(sel)
                if tag and text_norm(tag.get_text()):
                    break
            name = text_norm(tag.get_text()) if tag else ""
        if not name:
            continue
        parent = a.find_parent(["div", "li", "article", "section"]) or a
        img_src = extract_img_src(parent) or extract_img_src(a)
        det_seasons, det_weather = parse_detail_page(abs_href, browser=browser)
        if not img_src:
            try:
                det_soup = browser.soup(abs_href) if browser else get_soup(abs_href)
                header_img = det_soup.select_one("article img, main img, .header img, .infobox img")
                if header_img and header_img.get("src"):
                    img_src = header_img["src"]
            except Exception:
                pass
        items.append(
            ItemRow(
                image=img_src,
                name=name,
                seasons=set_join(det_seasons),
                weather=set_join(det_weather),
                source_page=url,
            )
        )
    # de-dup por nome
    by_name = {}
    for r in items:
        k = r.name.lower()
        if k not in by_name:
            by_name[k] = r
    return list(by_name.values())

# -----------------------------
# PT-BR data (arquivos locais)
# -----------------------------

ICON_BASE = "https://coral.guide/assets/live/items/icons/"
ICON_VERSION = "v1.2-1238"


def _icon_to_url(icon_name: str) -> str:
    if not icon_name:
        return ""
    # Usar WEBP com parâmetro de versão
    return f"{ICON_BASE}{icon_name}.webp?v={ICON_VERSION}"


def _season_flags_to_set(flags: dict) -> Set[str]:
    mapping = {
        "spring": "Spring",
        "summer": "Summer",
        "fall": "Fall",
        "autumn": "Fall",
        "winter": "Winter",
    }
    out: Set[str] = set()
    for k, v in flags.items():
        if v and k.lower() in mapping:
            out.add(mapping[k.lower()])
    return out


def _weather_flags_to_set(flags: dict) -> Set[str]:
    mapping = {
        "sunny": "Sunny",
        "rain": "Rainy",
        "storm": "Stormy",
        "windy": "Windy",
        "snow": "Snowy",
        "blizzard": "Snowy",
        "cloudy": "Cloudy",
    }
    out: Set[str] = set()
    for k, v in flags.items():
        if v and k.lower() in mapping:
            out.add(mapping[k.lower()])
    return out


def parse_ptbr_crops() -> List[ItemRow]:
    path = "pt-BR/crops.json"
    try:
        data = json.loads(open(path, "r", encoding="utf-8").read())
    except Exception:
        return []
    rows: List[ItemRow] = []
    for entry in data:
        # Nome: preferir o item colhível (pickupableItem), senão o item sementes
        pick = entry.get("pickupableItem") or {}
        itm = entry.get("item") or {}
        name = pick.get("displayName") or itm.get("displayName") or ""
        icon = pick.get("iconName") or itm.get("iconName") or ""
        seasons = set(entry.get("growableSeason") or [])
        rows.append(
            ItemRow(
                image=_icon_to_url(icon),
                name=name,
                seasons=set_join({s.title() for s in seasons}),
                weather="",
                source_page=path,
            )
        )
    return rows


def parse_ptbr_fish() -> List[ItemRow]:
    path = "pt-BR/fish.json"
    try:
        data = json.loads(open(path, "r", encoding="utf-8").read())
    except Exception:
        return []
    rows: List[ItemRow] = []
    for entry in data:
        item = entry.get("item") or {}
        name = item.get("displayName") or entry.get("fishName") or ""
        icon = item.get("iconName") or ""
        seasons: Set[str] = set()
        weather: Set[str] = set()
        for ss in entry.get("spawnSettings") or []:
            seasons |= _season_flags_to_set(ss.get("spawnSeason") or {})
            weather |= _weather_flags_to_set(ss.get("spawnWeather") or {})
        rows.append(
            ItemRow(
                image=_icon_to_url(icon),
                name=name,
                seasons=set_join(seasons),
                weather=set_join(weather),
                source_page=path,
            )
        )
    return rows


def parse_ptbr_insects() -> List[ItemRow]:
    path = "pt-BR/bugs-and-insects.json"
    try:
        data = json.loads(open(path, "r", encoding="utf-8").read())
    except Exception:
        return []
    rows: List[ItemRow] = []
    for entry in data:
        item = entry.get("item") or {}
        name = item.get("displayName") or entry.get("name") or ""
        icon = item.get("iconName") or ""
        seasons: Set[str] = set()
        weather: Set[str] = set()
        for ss in entry.get("spawnSettings") or []:
            seasons |= _season_flags_to_set(ss.get("spawnSeason") or {})
            weather |= _weather_flags_to_set(ss.get("spawnWeather") or {})
        rows.append(
            ItemRow(
                image=_icon_to_url(icon),
                name=name,
                seasons=set_join(seasons),
                weather=set_join(weather),
                source_page=path,
            )
        )
    return rows


def parse_ptbr_seacritters() -> List[ItemRow]:
    path = "pt-BR/ocean-critters.json"
    try:
        data = json.loads(open(path, "r", encoding="utf-8").read())
    except Exception:
        return []
    rows: List[ItemRow] = []
    for entry in data:
        item = entry.get("item") or {}
        name = item.get("displayName") or entry.get("name") or ""
        icon = item.get("iconName") or ""
        seasons: Set[str] = set()
        weather: Set[str] = set()
        for ss in entry.get("spawnSettings") or []:
            seasons |= _season_flags_to_set(ss.get("spawnSeason") or {})
            weather |= _weather_flags_to_set(ss.get("spawnWeather") or {})
        rows.append(
            ItemRow(
                image=_icon_to_url(icon),
                name=name,
                seasons=set_join(seasons),
                weather=set_join(weather),
                source_page=path,
            )
        )
    return rows


def scrape_ptbr() -> List[ItemRow]:
    # Índices auxiliares
    def load_json(path: str):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    items_list = load_json("pt-BR/items.json") or []
    items_by_id = {it.get("id"): it for it in items_list if isinstance(it, dict) and it.get("id")}

    # Mapas para seasons/weather vindos de arquivos específicos
    # Crops: map do produto colhido -> seasons
    crops_data = load_json("pt-BR/crops.json") or []
    crop_product_to_seasons: dict[str, Set[str]] = {}
    for c in crops_data:
        pick = (c or {}).get("pickupableItem") or {}
        pid = pick.get("id")
        if pid:
            crop_product_to_seasons[pid] = {s.title() for s in (c.get("growableSeason") or [])}

    # Fish usa lista spawnSettings; insetos e sea critters usam spawnSeason/spawnWeather no topo
    def map_spawn_list(file_path: str, item_id_field: str = "id") -> dict[str, Tuple[Set[str], Set[str]]]:
        data = load_json(file_path) or []
        out: dict[str, Tuple[Set[str], Set[str]]] = {}
        for e in data:
            item = e.get("item") or {}
            iid = item.get(item_id_field)
            if not iid:
                continue
            seasons: Set[str] = set()
            weather: Set[str] = set()
            for ss in e.get("spawnSettings") or []:
                seasons |= _season_flags_to_set(ss.get("spawnSeason") or {})
                weather |= _weather_flags_to_set(ss.get("spawnWeather") or {})
            out[iid] = (seasons, weather)
        return out

    def map_spawn_top(file_path: str, item_id_field: str = "id") -> dict[str, Tuple[Set[str], Set[str]]]:
        data = load_json(file_path) or []
        out: dict[str, Tuple[Set[str], Set[str]]] = {}
        for e in data:
            item = e.get("item") or {}
            iid = item.get(item_id_field)
            if not iid:
                continue
            seasons = _season_flags_to_set(e.get("spawnSeason") or {})
            weather = _weather_flags_to_set(e.get("spawnWeather") or {})
            out[iid] = (seasons, weather)
        return out

    fish_spawn = map_spawn_list("pt-BR/fish.json")
    insects_spawn = map_spawn_top("pt-BR/bugs-and-insects.json")
    seacritters_spawn = map_spawn_top("pt-BR/ocean-critters.json")

    def item_info(item_id: str) -> Tuple[str, str]:
        it = items_by_id.get(item_id) or {}
        name = it.get("displayName") or ""
        icon = it.get("iconName") or ""
        return name, _icon_to_url(icon)

    def parse_journal(path: str, category: str) -> List[ItemRow]:
        data = load_json(path) or []
        rows: List[ItemRow] = []
        # Mapa de rótulo pt-BR por categoria interna
        label_map = {
            "crops": "crops",
            "animalproducts": "animal products",
            "artisan": "artisan products",
            "ocean": "ocean",
            "fish": "peixes",
            "insects": "insetos",
            "seacritters": "animais marinhos",
            "artifacts": "artefatos",
            "gems": "gemas",
            "fossils": "fossil",
            "scavangables": "coletaveis",
        }
        for e in data:
            key = (e or {}).get("key")
            if not key:
                continue
            name, img_url = item_info(key)
            seasons: Set[str] = set()
            weather: Set[str] = set()
            # Enriquecer conforme categoria
            if category == "crops":
                seasons = set_join(crop_product_to_seasons.get(key, set())) and crop_product_to_seasons.get(key, set()) or set()
            elif category == "fish":
                s, w = fish_spawn.get(key, (set(), set()))
                seasons, weather = s, w
            elif category == "insects":
                s, w = insects_spawn.get(key, (set(), set()))
                seasons, weather = s, w
            elif category == "seacritters":
                s, w = seacritters_spawn.get(key, (set(), set()))
                seasons, weather = s, w
            elif category == "ocean":
                # ocean products: usar seasons de crops quando disponíveis
                seasons = crop_product_to_seasons.get(key, set())
            # Demais categorias não têm seasons/weather evidentes nos JSONs
            rows.append(
                ItemRow(
                    image=img_url,
                    name=name or key,
                    seasons=set_join(seasons),
                    weather=set_join(weather),
                    source_page=path,
                    category=label_map.get(category, category),
                )
            )
        return rows

    categories = [
        ("pt-BR/journal-crops.json", "crops"),
        ("pt-BR/journal-animal-products.json", "animalproducts"),
        ("pt-BR/journal-artisan-products.json", "artisan"),
        ("pt-BR/journal-ocean-products.json", "ocean"),
        ("pt-BR/journal-fish.json", "fish"),
        ("pt-BR/journal-insects.json", "insects"),
        ("pt-BR/journal-sea-critters.json", "seacritters"),
        ("pt-BR/journal-artifacts.json", "artifacts"),
        ("pt-BR/journal-gems.json", "gems"),
        ("pt-BR/journal-fossils.json", "fossils"),
        ("pt-BR/journal-scavangable.json", "scavangables"),
    ]

    all_rows: List[ItemRow] = []
    for path, cat in categories:
        rows = parse_journal(path, cat)
        print(f"[pt-BR] {path}: {len(rows)} itens")
        all_rows.extend(rows)

    # de-dup global por nome
    by_name: dict[str, ItemRow] = {}
    for r in all_rows:
        k = r.name.lower()
        if k not in by_name:
            by_name[k] = r
        else:
            prev = by_name[k]
            seasons = set(prev.seasons.split("; ")) if prev.seasons else set()
            seasons |= set(r.seasons.split("; ")) if r.seasons else set()
            weather = set(prev.weather.split("; ")) if prev.weather else set()
            weather |= set(r.weather.split("; ")) if r.weather else set()
            image = prev.image or r.image
            category = prev.category or r.category
            by_name[k] = ItemRow(image=image, name=prev.name, seasons=set_join(seasons), weather=set_join(weather), source_page=prev.source_page, category=category)

    # ordenar pela categoria na ordem especificada e depois por nome
    order = [
        "crops",
        "animal products",
        "artisan products",
        "ocean",
        "peixes",
        "insetos",
        "animais marinhos",
        "artefatos",
        "gemas",
        "fossil",
        "coletaveis",
    ]
    order_index = {cat: i for i, cat in enumerate(order)}
    result = list(by_name.values())
    result.sort(key=lambda r: (order_index.get(r.category, 999), r.name.lower()))
    return result


def scrape_all(urls: List[str]) -> List[ItemRow]:
    all_rows: List[ItemRow] = []
    with BrowserSession(headless=True) as browser:
        use_browser = browser is not None
        for i, url in enumerate(urls, start=1):
            print(f"[{i}/{len(urls)}] Fetching: {url} {'(browser)' if use_browser else '(http)'}")
            try:
                rows = parse_list_page(url, browser=browser if use_browser else None)
                print(f"  -> {len(rows)} items")
                all_rows.extend(rows)
            except Exception as e:
                print(f"  !! Failed {url}: {e}")
            time.sleep(0.4)  # be polite
        if len(all_rows) == 0:
            try:
                # tentar capturar JSON da página de database
                if use_browser:
                    print("[fallback] Tentando capturar JSON de /database via XHR...")
                    payloads = browser.capture_json_from_page("https://coral.guide/database")
                    # heurística: procurar objetos de itens com campos de nome/ícone
                    def walk(obj):
                        if isinstance(obj, dict):
                            yield obj
                            for v in obj.values():
                                yield from walk(v)
                        elif isinstance(obj, list):
                            for v in obj:
                                yield from walk(v)
                    candidates = []
                    for _, data in payloads:
                        for node in walk(data):
                            keys = set(map(str.lower, node.keys())) if isinstance(node, dict) else set()
                            if {"name", "icon"} & keys or {"name", "image"} & keys:
                                candidates.append(node)
                    if candidates:
                        print(f"[fallback] Encontrados {len(candidates)} candidatos de itens em JSON")
                        for node in candidates:
                            name = text_norm(str(node.get("name", "")))
                            image = str(node.get("icon") or node.get("image") or "")
                            seasons = node.get("seasons") or node.get("season") or []
                            weather = node.get("weather") or []
                            if isinstance(seasons, str):
                                seasons_list = {seasons}
                            else:
                                seasons_list = {str(s) for s in seasons}
                            if isinstance(weather, str):
                                weather_list = {weather}
                            else:
                                weather_list = {str(w) for w in weather}
                            all_rows.append(ItemRow(image=image, name=name, seasons=set_join(seasons_list), weather=set_join(weather_list), source_page="/database-json"))
                if len(all_rows) == 0:
                    fallback_rows = parse_database_index(browser=browser if use_browser else None)
                    print(f"[fallback] Itens do índice do database: {len(fallback_rows)}")
                    all_rows.extend(fallback_rows)
            except Exception as e:
                print(f"[fallback] Falhou ao carregar /database: {e}")
    # Deduplicate globally by (name)
    by_name = {}
    for r in all_rows:
        key = r.name.lower()
        if key not in by_name:
            by_name[key] = r
        else:
            prev = by_name[key]
            # Merge fields
            seasons = set(prev.seasons.split("; ")) if prev.seasons else set()
            seasons |= set(r.seasons.split("; ")) if r.seasons else set()
            weather = set(prev.weather.split("; ")) if prev.weather else set()
            weather |= set(r.weather.split("; ")) if r.weather else set()
            image = prev.image or r.image
            by_name[key] = ItemRow(image=image, name=prev.name, seasons=set_join(seasons), weather=set_join(weather), source_page=prev.source_page)
    return list(by_name.values())


def write_csv(rows: List[ItemRow], out_path: str) -> None:
    # Cabeçalho em pt-BR conforme solicitado
    fieldnames = ["url da imagem", "nome", "seasons", "weather", "categoria"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "url da imagem": r.image,
                "nome": r.name,
                "seasons": r.seasons,
                "weather": r.weather,
                "categoria": r.category,
            })


def _image_url_from_tag(img: Tag) -> str:
    # Fandom usa lazy loading: data-src ou srcset
    if img is None:
        return ""
    # srcset: pegue o primeiro URL
    srcset = img.get("srcset") or img.get("data-srcset")
    if srcset:
        # formato: "https://... 1x, https://... 2x"
        first = srcset.split(",")[0].strip().split(" ")[0]
        if first and not first.startswith("data:"):
            return first
    # data-src / data-original
    for attr in ["data-src", "data-original", "data-lazy-src", "src"]:
        val = img.get(attr)
        if val and not val.startswith("data:"):
            return val
    return img.get("src") or ""


def parse_fandom_table(url: str) -> List[ItemRow]:
    soup = get_soup(url)
    rows: List[ItemRow] = []
    tables = soup.select("table.wikitable, table.article-table, table")
    for table in tables:
        trs = table.select("tr")
        if len(trs) < 3:
            continue
        headers = [text_norm(th.get_text()) for th in trs[0].select("th")] if trs else []
        for tr in trs[1:]:
            tds = tr.select("td")
            if not tds:
                continue
            img_src = ""
            img = tr.select_one("img")
            if img:
                img_src = _image_url_from_tag(img)
            name = ""
            a = tr.select_one("a[title]") or tr.select_one("a")
            if a:
                name = text_norm(a.get("title") or a.get_text())
            if not name:
                name = text_norm(tds[0].get_text())
            if not name or name.lower() in {"image", "icon", "name"}:
                continue
            tr_text = text_norm(tr.get_text(" ", strip=True))
            seasons = find_by_labels(tr_text, SEASON_WORDS)
            weather = find_by_labels(tr_text, WEATHER_WORDS)
            for icon in tr.select("img, [title], [aria-label]"):
                meta = " ".join(
                    filter(
                        None,
                        [
                            icon.get("alt", "") if hasattr(icon, "get") else "",
                            icon.get("title", "") if hasattr(icon, "get") else "",
                            icon.get("aria-label", "") if hasattr(icon, "get") else "",
                        ],
                    )
                )
                meta = text_norm(meta)
                if meta:
                    seasons |= find_by_labels(meta, SEASON_WORDS)
                    weather |= find_by_labels(meta, WEATHER_WORDS)
            rows.append(
                ItemRow(
                    image=img_src,
                    name=name,
                    seasons=set_join(seasons),
                    weather=set_join(weather),
                    source_page=url,
                )
            )
    best = {}
    for r in rows:
        k = r.name.lower()
        if k not in best:
            best[k] = r
        else:
            prev = best[k]
            prev_score = (len(prev.seasons) > 0) + (len(prev.weather) > 0) + (len(prev.image) > 0)
            new_score = (len(r.seasons) > 0) + (len(r.weather) > 0) + (len(r.image) > 0)
            if new_score > prev_score:
                best[k] = r
    return list(best.values())


def scrape_fandom(urls: List[str]) -> List[ItemRow]:
    all_rows: List[ItemRow] = []
    for i, url in enumerate(urls, start=1):
        print(f"[FANDOM {i}/{len(urls)}] {url}")
        try:
            rows = parse_fandom_table(url)
            print(f"  -> {len(rows)} itens")
            all_rows.extend(rows)
        except Exception as e:
            print(f"  !! Falhou {url}: {e}")
        time.sleep(0.5)
    by_name = {}
    for r in all_rows:
        key = r.name.lower()
        if key not in by_name:
            by_name[key] = r
        else:
            prev = by_name[key]
            seasons = set(prev.seasons.split("; ")) if prev.seasons else set()
            seasons |= set(r.seasons.split("; ")) if r.seasons else set()
            weather = set(prev.weather.split("; ")) if prev.weather else set()
            weather |= set(r.weather.split("; ")) if r.weather else set()
            image = prev.image or r.image
            by_name[key] = ItemRow(
                image=image,
                name=prev.name,
                seasons=set_join(seasons),
                weather=set_join(weather),
                source_page=prev.source_page,
            )
    return list(by_name.values())
def main():
    # Usar dados locais pt-BR para gerar CSV nas colunas solicitadas
    rows = scrape_ptbr()
    print(f"Total items: {len(rows)}")
    write_csv(rows, "coral_island.csv")
    print("CSV salvo em coral_island.csv")


if __name__ == "__main__":
    main()

