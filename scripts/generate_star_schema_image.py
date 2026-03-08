from PIL import Image, ImageDraw, ImageFont


W, H = 1500, 900
BG = (38, 41, 48)
BOX = (57, 62, 70)
BORDER = (176, 180, 188)
TEXT = (235, 237, 240)
LINE = (173, 178, 186)


def draw_box(draw, rect, label, font):
    x1, y1, x2, y2 = rect
    draw.rectangle(rect, fill=BOX, outline=BORDER, width=3)
    draw.text((x1 + 16, y1 + 16), label, fill=TEXT, font=font)


def h_connector(draw, x1, y1, x2, y2, mid_x):
    draw.line((x1, y1, mid_x, y1), fill=LINE, width=3)
    draw.line((mid_x, y1, mid_x, y2), fill=LINE, width=3)
    draw.line((mid_x, y2, x2, y2), fill=LINE, width=3)


img = Image.new("RGB", (W, H), BG)
draw = ImageDraw.Draw(img)

try:
    title_font = ImageFont.truetype("arial.ttf", 40)
    box_font = ImageFont.truetype("arial.ttf", 24)
    small_font = ImageFont.truetype("arial.ttf", 20)
except Exception:
    title_font = ImageFont.load_default()
    box_font = ImageFont.load_default()
    small_font = ImageFont.load_default()

draw.text((24, 20), "Esquema de estrella - Gold (NYC Trips)", fill=TEXT, font=title_font)

# Boxes
fact = (560, 325, 965, 560)
dim_date = (110, 150, 430, 270)
dim_pu = (110, 335, 430, 455)
dim_do = (110, 520, 430, 640)
dim_service = (1070, 120, 1410, 230)
dim_payment = (1070, 265, 1410, 375)
dim_vendor = (1070, 410, 1410, 520)
dim_rate = (1070, 555, 1410, 665)

draw_box(draw, fact, "Tabla de hechos: gold.fct_trips", box_font)
draw.text((580, 390), "pickup_date_key, pu_zone_key, do_zone_key", fill=TEXT, font=small_font)
draw.text((580, 425), "service_type, payment_type_id", fill=TEXT, font=small_font)
draw.text((580, 460), "vendor_id, ratecode_id, metricas", fill=TEXT, font=small_font)

draw_box(draw, dim_date, "Dim: gold.dim_date", box_font)
draw_box(draw, dim_pu, "Dim: gold.dim_zone (PU)", box_font)
draw_box(draw, dim_do, "Dim: gold.dim_zone (DO)", box_font)
draw_box(draw, dim_service, "Dim: gold.dim_service_type", box_font)
draw_box(draw, dim_payment, "Dim: gold.dim_payment_type", box_font)
draw_box(draw, dim_vendor, "Dim: gold.dim_vendor", box_font)
draw_box(draw, dim_rate, "Dim: gold.dim_rate_code", box_font)

# Connectors (left)
mid_left = 500
h_connector(draw, dim_date[2], (dim_date[1] + dim_date[3]) // 2, fact[0], 375, mid_left)
h_connector(draw, dim_pu[2], (dim_pu[1] + dim_pu[3]) // 2, fact[0], 430, mid_left)
h_connector(draw, dim_do[2], (dim_do[1] + dim_do[3]) // 2, fact[0], 485, mid_left)

# Connectors (right)
mid_right = 1020
h_connector(draw, fact[2], 375, dim_service[0], (dim_service[1] + dim_service[3]) // 2, mid_right)
h_connector(draw, fact[2], 430, dim_payment[0], (dim_payment[1] + dim_payment[3]) // 2, mid_right)
h_connector(draw, fact[2], 485, dim_vendor[0], (dim_vendor[1] + dim_vendor[3]) // 2, mid_right)
h_connector(draw, fact[2], 540, dim_rate[0], (dim_rate[1] + dim_rate[3]) // 2, mid_right)

# Labels on lines
draw.text((438, 350), "pickup_date_key", fill=TEXT, font=small_font)
draw.text((450, 405), "pu_zone_key", fill=TEXT, font=small_font)
draw.text((450, 515), "do_zone_key", fill=TEXT, font=small_font)
draw.text((970, 340), "service_type", fill=TEXT, font=small_font)
draw.text((970, 430), "payment_type_id", fill=TEXT, font=small_font)
draw.text((970, 490), "vendor_id", fill=TEXT, font=small_font)
draw.text((970, 575), "ratecode_id", fill=TEXT, font=small_font)

out = r"F:/Deber2/Evidencias/Esquema Estrella Gold v2.png"
img.save(out)
print(out)
