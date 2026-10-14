import pathlib
from playwright.sync_api import sync_playwright
here = pathlib.Path(__file__).parent.resolve()
out = pathlib.Path("/home/user/trading-bot/media")
with sync_playwright() as p:
    b = p.chromium.launch(executable_path="/opt/pw-browsers/chromium-1194/chrome-linux/chrome")
    pg = b.new_page(viewport={"width": 1500, "height": 1600})
    pg.goto((here / "brand.html").as_uri()); pg.wait_for_function("document.fonts.ready.then(()=>true)"); pg.wait_for_timeout(400)
    pg.locator("#pfp").screenshot(path=str(out / "spark-avatar.png"))
    pg.locator("#ban").screenshot(path=str(out / "spark-header.png"))
    b.close()
