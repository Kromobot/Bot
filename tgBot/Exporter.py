# -*- coding: utf-8 -*-
# Экспорт YouTube/Google кук из профиля браузера в Netscape cookies.txt
# Пример: python export_youtube_cookies.py chrome Default cookies.txt
import sys, time
from http.cookiejar import MozillaCookieJar, Cookie
from yt_dlp.cookies import extract_cookies_from_browser

DOMAINS = [".youtube.com", ".google.com", ".youtube-nocookie.com"]

def to_mozilla_cj(src_cj) -> MozillaCookieJar:
    cj = MozillaCookieJar()
    now = int(time.time())
    for c in src_cj:
        # фильтруем только нужные домены
        if not any(c.domain.endswith(d) or ("."+c.domain).endswith(d) for d in DOMAINS):
            continue
        expires = int(c.expires) if c.expires else now + 3600*24*180
        cookie = Cookie(
            version=0, name=c.name, value=c.value, port=None, port_specified=False,
            domain=c.domain, domain_specified=True, domain_initial_dot=c.domain.startswith("."),
            path=c.path or "/", path_specified=True, secure=c.secure, expires=expires,
            discard=False, comment=None, comment_url=None, rest={}, rfc2109=False
        )
        cj.set_cookie(cookie)
    return cj

def main():
    if len(sys.argv) < 4:
        print("Usage: export_youtube_cookies.py <browser> <profile> <out_cookies.txt>")
        print("browser: chrome|edge|firefox|brave|vivaldi|opera")
        sys.exit(2)
    browser, profile, out_file = sys.argv[1], sys.argv[2], sys.argv[3]
    src_cj = extract_cookies_from_browser(browser=browser, profile=profile)
    cj = to_mozilla_cj(src_cj)
    cj.save(out_file, ignore_discard=True, ignore_expires=True)
    print(f"Saved: {out_file} ({len(cj)} cookies)")

if __name__ == "__main__":
    main()
