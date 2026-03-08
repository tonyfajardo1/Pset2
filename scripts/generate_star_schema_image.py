from PIL import Image, ImageDraw, ImageFont


W, H = 1400, 820
BG = (40, 42, 46)
BOX = (54, 57, 63)
BORDER = (170, 170, 170)
TEXT = (235, 235, 235)
LINE = (160, 160, 160)


def draw_box(draw, x, y, w, h, label, font, title=False):
    draw.rectangle((x, y, x + w, y + h), fill=BOX, outline=BORDER, width=2)
    if title:
        draw.text((x + 20, y + 16), label, fill=TEXT, font=font)
    else:
        draw.text((x + 16, y + 18), label, fill=TEXT, font=font)


img = Image.new("RGB", (W, H), BG)
draw = ImageDraw.Draw(img)

try:
    title_font = ImageFont.truetype("arial.ttf", 32)
    box_font = ImageFont.truetype("arial.ttf", 22)
    small_font = ImageFont.truetype("arial.ttf", 18)
except Exception:
    title_font = ImageFont.load_default()
    box_font = ImageFont.load_default()
    small_font = ImageFont.load_default()

draw.text((24, 20), "Esquema de estrella - Gold (NYC Trips)", fill=TEXT, font=title_font)

# Center fact box
fx, fy, fw, fh = 520, 300, 360, 190
draw_box(draw, fx, fy, fw, fh, "Tabla de hechos: gold.fct_trips", box_font, title=True)
draw.text((fx + 20, fy + 66), "pickup_date_key, pu_zone_key, do_zone_key", fill=TEXT, font=small_font)
draw.text((fx + 20, fy + 94), "service_type, payment_type_id", fill=TEXT, font=small_font)
draw.text((fx + 20, fy + 122), "vendor_id, ratecode_id, medidas", fill=TEXT, font=small_font)

# Dimension boxes
draw_box(draw, 110, 160, 300, 110, "Dim: gold.dim_date", box_font)
draw_box(draw, 110, 300, 300, 110, "Dim: gold.dim_zone (PU)", box_font)
draw_box(draw, 110, 440, 300, 110, "Dim: gold.dim_zone (DO)", box_font)

draw_box(draw, 990, 120, 300, 105, "Dim: gold.dim_service_type", box_font)
draw_box(draw, 990, 245, 300, 105, "Dim: gold.dim_payment_type", box_font)
draw_box(draw, 990, 370, 300, 105, "Dim: gold.dim_vendor", box_font)
draw_box(draw, 990, 495, 300, 105, "Dim: gold.dim_rate_code", box_font)

# Left connectors
draw.line((410, 215, 520, 335), fill=LINE, width=3)
draw.line((410, 355, 520, 365), fill=LINE, width=3)
draw.line((410, 495, 520, 395), fill=LINE, width=3)

# Right connectors
draw.line((880, 335, 990, 170), fill=LINE, width=3)
draw.line((880, 365, 990, 295), fill=LINE, width=3)
draw.line((880, 395, 990, 420), fill=LINE, width=3)
draw.line((880, 425, 990, 545), fill=LINE, width=3)

# Labels on connectors
draw.text((430, 250), "pickup_date_key", fill=TEXT, font=small_font)
draw.text((430, 350), "pu_zone_key", fill=TEXT, font=small_font)
draw.text((430, 470), "do_zone_key", fill=TEXT, font=small_font)
draw.text((900, 225), "service_type", fill=TEXT, font=small_font)
draw.text((900, 315), "payment_type_id", fill=TEXT, font=small_font)
draw.text((900, 430), "vendor_id", fill=TEXT, font=small_font)
draw.text((900, 535), "ratecode_id", fill=TEXT, font=small_font)

out = r"F:/Deber2/Evidencias/Esquema Estrella Gold.png"
img.save(out)
print(out)
