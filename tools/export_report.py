"""
Export the report (docs/rapport.md) to Word and PDF for submission.

Steps:
1. every Mermaid diagram is drawn by headless Firefox and saved as a PNG, white margins
   cropped (Firefox and an internet connection for the Mermaid library are needed);
2. the Markdown becomes HTML (markdown-it-py), with the diagrams as images sized to the
   page width, and a print layout (fonts, tables with lines);
3. LibreOffice converts the HTML to .docx; the page is then set to A4 with 2.2 cm margins
   inside the .docx (LibreOffice's HTML import ignores page sizes), and the headings'
   built-in indent is removed;
4. LibreOffice converts that .docx to .pdf, so both look the same.

The result is in build/ (not in git): build/rapport.docx, build/rapport.pdf, and
build/rapport.html. Edit docs/rapport.md, not the exports, and run this again.

Usage
-----
    .venv/bin/python -m tools.export_report
"""
from __future__ import annotations

import base64
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "rapport.md"
OUT = ROOT / "build"
MERMAID = "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"

TEXT_WIDTH_PX = 600          # an image's maximum width: ~16 cm, the A4 text width at 96 dpi
CSS = """
body { font-family: 'Liberation Sans', Arial, sans-serif; font-size: 10.5pt; line-height: 1.4;
       color: #111; }
h1, h2, h3 { font-family: 'Liberation Sans', Arial, sans-serif; margin-left: 0;
             padding-left: 0; text-indent: 0; }
h1 { font-size: 20pt; margin: 0 0 4pt; }
h2 { font-size: 15pt; margin: 18pt 0 6pt; page-break-after: avoid; }
h3 { font-size: 12pt; margin: 12pt 0 4pt; page-break-after: avoid; }
table { border-collapse: collapse; width: 100%; margin: 6pt 0 10pt; font-size: 9pt; }
th, td { border: 0.5pt solid #999; padding: 3pt 5pt; vertical-align: top; text-align: left;
         line-height: 1.2; }
th { background: #eeeeee; }
img { max-width: 100%; }
code { font-family: 'Liberation Mono', monospace; font-size: 9pt; }
hr { border: 0; border-top: 0.5pt solid #bbb; }
"""


def render_diagram(code: str, workdir: Path) -> bytes:
    """One Mermaid diagram as a cropped PNG, drawn by headless Firefox."""
    from PIL import Image, ImageChops
    page = workdir / "diagram.html"
    page.write_text(f"""<!doctype html><html><body style="background:#fff;margin:0">
<pre class="mermaid">{code}</pre><script src="{MERMAID}"></script>
<script>mermaid.initialize({{startOnLoad:true, flowchart:{{useMaxWidth:false}}}});</script>
</body></html>""", encoding="utf-8")
    shot = workdir / "diagram.png"
    shot.unlink(missing_ok=True)
    profile = workdir / "profile"
    profile.mkdir(exist_ok=True)
    subprocess.run(["firefox", "--headless", "--no-remote", "--profile", str(profile),
                    "--window-size=1400,1400", "--screenshot", str(shot), page.as_uri()],
                   capture_output=True, timeout=120)
    if not shot.exists():
        raise RuntimeError("Firefox made no screenshot of the diagram")
    image = Image.open(shot).convert("RGB")
    box = ImageChops.difference(image, Image.new("RGB", image.size, "white")).getbbox()
    if box is None:
        raise RuntimeError("the diagram came out empty (no internet for the Mermaid library?)")
    image = image.crop((max(box[0] - 10, 0), max(box[1] - 10, 0), box[2] + 10, box[3] + 10))
    out = workdir / "cropped.png"
    image.save(out)
    return out.read_bytes()


def _first_column_width(table: re.Match) -> str:
    """LibreOffice makes a first column as narrow as it can, breaking words in it
    ("bandbreedt-e"), and ignores `nowrap`. When the first column holds short labels, give
    it a width that fits the longest label (about 1.8% of the text width per character)."""
    html = table[0]
    cells = re.findall(r"<tr>\s*<(?:td|th)>(.*?)</(?:td|th)>", html, flags=re.S)
    longest = max((len(re.sub(r"<[^>]+>", "", c)) for c in cells), default=0)
    if not cells or longest > 24:       # long first columns (e.g. file paths) may wrap
        return html
    width = min(35, round(longest * 1.8 + 4))
    return re.sub(r"<tr>\s*<(td|th)>", lambda m: f'<tr><{m[1]} width="{width}%">', html)


def to_html(markdown_text: str, workdir: Path) -> str:
    """The report as one self-contained HTML page (diagrams embedded as images)."""
    from markdown_it import MarkdownIt

    def diagram(match: re.Match) -> str:
        import io
        from PIL import Image
        png = render_diagram(match.group(1), workdir)
        w, h = Image.open(io.BytesIO(png)).size
        scale = min(1.0, TEXT_WIDTH_PX / w)             # fit the page width, keep the ratio
        data = base64.b64encode(png).decode()
        return (f'\n<p><img src="data:image/png;base64,{data}" alt="diagram" '
                f'width="{round(w * scale)}" height="{round(h * scale)}"></p>\n')

    text = re.sub(r"```mermaid\n(.*?)```", diagram, markdown_text, flags=re.S)
    body = MarkdownIt("commonmark", {"html": True}).enable("table").render(text)
    # a relative width, so tables follow the page width (set to A4 after the import)
    body = body.replace("<table>", '<table width="100%" border="1" cellpadding="4" cellspacing="0">')
    body = re.sub(r"<table .*?</table>", _first_column_width, body, flags=re.S)
    # some space between a table and the text below it
    body = body.replace("</table>", '</table>\n<p style="font-size:4pt">&nbsp;</p>')
    return (f'<!doctype html><html lang="nl"><head><meta charset="utf-8"><title>Rapport</title>'
            f"<style>{CSS}</style></head><body>{body}</body></html>")


def soffice(source: Path, target: str, infilter: str | None = None) -> None:
    cmd = ["soffice", "--headless", "--convert-to", target, "--outdir", str(OUT), str(source)]
    if infilter:
        cmd[2:2] = [f"--infilter={infilter}"]
    subprocess.run(cmd, check=True, capture_output=True, timeout=180)


def finish_docx(docx: Path, margin_cm: float = 2.2) -> None:
    """Set every section of a .docx to A4 portrait with the given margins (in twips:
    1 cm = 567), keep every table row on one page, and remove the left indent LibreOffice
    gives headings 2 and 3, by editing word/document.xml and word/styles.xml in the zip."""
    import zipfile
    twips = round(margin_cm * 567)
    tmp = docx.with_suffix(".tmp")
    with zipfile.ZipFile(docx) as src, zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as dst:
        for item in src.infolist():
            data = src.read(item.filename)
            if item.filename == "word/document.xml":
                xml = data.decode("utf-8")
                xml = re.sub(r"<w:pgSz [^>]*/>", '<w:pgSz w:w="11906" w:h="16838"/>', xml)
                xml = re.sub(r"<w:pgMar [^>]*/>",
                             f'<w:pgMar w:top="{twips}" w:right="{twips}" w:bottom="{twips}" '
                             f'w:left="{twips}" w:header="0" w:footer="0" w:gutter="0"/>', xml)
                # a table row is never split across two pages
                xml = re.sub(r"<w:tr>(?!<w:trPr>)", "<w:tr><w:trPr><w:cantSplit/></w:trPr>", xml)
                xml = re.sub(r"<w:tr><w:trPr>(?!<w:cantSplit/>)", "<w:tr><w:trPr><w:cantSplit/>", xml)
                data = xml.encode("utf-8")
            elif item.filename == "word/styles.xml":
                xml = data.decode("utf-8")
                xml = re.sub(r'(<w:style [^>]*w:styleId="Heading[23]".*?)<w:ind [^>]*/>',
                             r"\1", xml, flags=re.S)
                data = xml.encode("utf-8")
            dst.writestr(item, data)
    tmp.replace(docx)


def main() -> int:
    for tool in ("firefox", "soffice"):
        if not shutil.which(tool):
            print(f"{tool} is needed for the export but was not found", file=sys.stderr)
            return 1
    OUT.mkdir(exist_ok=True)
    # Firefox installed as a snap can only use non-hidden folders in the home folder
    with tempfile.TemporaryDirectory(dir=Path.home(), prefix="report_export_") as tmp:
        html = to_html(SOURCE.read_text(encoding="utf-8"), Path(tmp))
    html_file = OUT / "rapport.html"
    html_file.write_text(html, encoding="utf-8")
    soffice(html_file, "docx:MS Word 2007 XML", infilter="HTML (StarWriter)")
    docx = OUT / "rapport.docx"
    finish_docx(docx)
    soffice(docx, "pdf:writer_pdf_Export")
    for f in (docx, OUT / "rapport.pdf"):
        print(f"wrote {f.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
