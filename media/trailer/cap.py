import sys, subprocess, pathlib
from playwright.sync_api import sync_playwright
here = pathlib.Path(__file__).parent.resolve()
mode = sys.argv[1]
with sync_playwright() as p:
    b = p.chromium.launch(executable_path="/opt/pw-browsers/chromium-1194/chrome-linux/chrome",
                          args=["--font-render-hinting=none"])
    pg = b.new_page(viewport={"width": 1920, "height": 1080})
    errs = []
    pg.on("pageerror", lambda e: errs.append(str(e)))
    pg.goto((here / "trailer.html").as_uri())
    pg.wait_for_function("document.fonts.ready.then(()=>true)")
    pg.wait_for_timeout(500)
    if mode == "preview":
        for t in [float(x) for x in sys.argv[2:]]:
            pg.evaluate(f"render({t})")
            pg.screenshot(path=str(here / f"f_{t:05.1f}.png"))
        print("errors:", errs)
    else:
        fps, dur, out = 30, 37.0, sys.argv[2]
        ff = subprocess.Popen(["ffmpeg", "-y", "-loglevel", "error", "-f", "image2pipe", "-framerate", str(fps), "-i", "-",
                               "-c:v", "libx264", "-preset", "slow", "-crf", "16", "-pix_fmt", "yuv420p",
                               "-profile:v", "high", "-movflags", "+faststart", out], stdin=subprocess.PIPE)
        n = int(dur * fps)
        for i in range(n):
            pg.evaluate(f"render({i / fps})")
            ff.stdin.write(pg.screenshot(type="png"))
            if i % 150 == 0:
                print("frame", i, "/", n, flush=True)
        ff.stdin.close(); ff.wait()
        print("errors:", errs, "ffmpeg exit", ff.returncode)
    b.close()
