"""Reference data used to mock a Vietnamese e-commerce shop."""

LAST_NAMES = [
    ("Nguyễn", 31), ("Trần", 11), ("Lê", 9), ("Phạm", 7), ("Hoàng", 5), ("Huỳnh", 4),
    ("Phan", 4), ("Vũ", 4), ("Võ", 4), ("Đặng", 2), ("Bùi", 2), ("Đỗ", 2),
    ("Hồ", 2), ("Ngô", 2), ("Dương", 1), ("Lý", 1), ("Trương", 1), ("Đinh", 1),
]
MIDDLE_NAMES_MALE = ["Văn", "Hữu", "Minh", "Đức", "Quang", "Thành", "Công", "Hoàng", "Gia", "Anh"]
MIDDLE_NAMES_FEMALE = ["Thị", "Ngọc", "Thu", "Thanh", "Minh", "Phương", "Kim", "Bảo", "Mai", "Hồng"]
FIRST_NAMES_MALE = [
    "An", "Bảo", "Cường", "Dũng", "Duy", "Đạt", "Hải", "Hiếu", "Hùng", "Huy", "Khang",
    "Khoa", "Long", "Minh", "Nam", "Nghĩa", "Phong", "Phúc", "Quân", "Sơn", "Tài",
    "Thắng", "Thịnh", "Trung", "Tuấn", "Việt", "Vinh",
]
FIRST_NAMES_FEMALE = [
    "Anh", "Chi", "Dung", "Giang", "Hà", "Hạnh", "Hoa", "Hương", "Lan", "Linh", "Ly",
    "Mai", "My", "Ngân", "Ngọc", "Nhung", "Oanh", "Phương", "Quỳnh", "Tâm", "Thảo",
    "Trang", "Uyên", "Vân", "Vy", "Yến",
]
EMAIL_DOMAINS = [("gmail.com", 70), ("yahoo.com", 10), ("outlook.com", 8), ("icloud.com", 7), ("fpt.vn", 5)]
PHONE_PREFIXES = ["032", "033", "034", "035", "036", "037", "038", "039", "070", "076", "077",
                  "078", "079", "081", "082", "083", "084", "085", "086", "088", "089", "090",
                  "091", "093", "094", "096", "097", "098"]

STREETS = [
    "Lê Lợi", "Nguyễn Huệ", "Trần Hưng Đạo", "Hai Bà Trưng", "Lý Thường Kiệt", "Điện Biên Phủ",
    "Nguyễn Trãi", "Lê Duẩn", "Phan Đình Phùng", "Hoàng Diệu", "Quang Trung", "Nguyễn Văn Linh",
    "Võ Văn Kiệt", "Cách Mạng Tháng Tám", "Bà Triệu", "Trần Phú", "Nguyễn Thị Minh Khai",
    "Lê Văn Sỹ", "Phạm Văn Đồng", "Hùng Vương", "Nguyễn Văn Cừ", "Tôn Đức Thắng",
]

# (city, weight) — thị phần đơn hàng theo tỉnh/thành
CITIES = [
    ("TP. Hồ Chí Minh", 30), ("Hà Nội", 24), ("Đà Nẵng", 5), ("Hải Phòng", 4), ("Cần Thơ", 3),
    ("Đồng Nai", 4), ("Bắc Ninh", 3), ("Quảng Ninh", 2), ("Thái Nguyên", 2), ("Ninh Bình", 2),
    ("Thanh Hóa", 2), ("Nghệ An", 2), ("Huế", 2), ("Khánh Hòa", 2), ("Gia Lai", 1),
    ("Đắk Lắk", 1), ("Lâm Đồng", 2), ("Tây Ninh", 2), ("An Giang", 2), ("Vĩnh Long", 1),
]

# Cách viết "bẩn" thường gặp khi người dùng tự nhập -> ETL phải chuẩn hóa
CITY_DIRTY_VARIANTS = {
    "TP. Hồ Chí Minh": ["TP HCM", "Hồ Chí Minh", "tp.hcm", "Sài Gòn", "HCM"],
    "Hà Nội": ["Ha Noi", "HN", "hà nội", "TP. Hà Nội"],
    "Đà Nẵng": ["Da Nang", "đà nẵng"],
    "Hải Phòng": ["Hai Phong"],
    "Cần Thơ": ["Can Tho"],
}

# Cây danh mục: (danh mục cha, [(danh mục con, thương hiệu, loại sản phẩm, (giá min, giá max))])
CATALOG = [
    ("Điện thoại & Phụ kiện", [
        ("Điện thoại", ["Apple", "Samsung", "Xiaomi", "OPPO", "Vivo", "Realme"],
         ["Điện thoại"], (2_500_000, 34_000_000)),
        ("Máy tính bảng", ["Apple", "Samsung", "Xiaomi", "Lenovo"],
         ["Máy tính bảng"], (3_000_000, 28_000_000)),
        ("Tai nghe", ["Sony", "JBL", "Apple", "Samsung", "Soundpeats"],
         ["Tai nghe Bluetooth", "Tai nghe chụp tai", "Tai nghe True Wireless"], (250_000, 7_000_000)),
        ("Sạc & Cáp", ["Anker", "Baseus", "Ugreen", "Xiaomi"],
         ["Củ sạc nhanh", "Cáp sạc Type-C", "Pin sạc dự phòng"], (90_000, 1_200_000)),
    ]),
    ("Máy tính & Laptop", [
        ("Laptop", ["Dell", "HP", "Asus", "Lenovo", "Acer", "Apple"],
         ["Laptop"], (9_000_000, 45_000_000)),
        ("Màn hình", ["LG", "Samsung", "Dell", "ViewSonic"],
         ["Màn hình"], (2_500_000, 15_000_000)),
        ("Bàn phím & Chuột", ["Logitech", "Razer", "Akko", "DareU"],
         ["Bàn phím cơ", "Chuột không dây", "Chuột gaming"], (150_000, 3_500_000)),
    ]),
    ("Thời trang", [
        ("Thời trang nam", ["Coolmate", "Routine", "Owen", "Yody"],
         ["Áo thun nam", "Áo sơ mi nam", "Quần jean nam", "Quần short nam"], (120_000, 800_000)),
        ("Thời trang nữ", ["IVY moda", "Elise", "Canifa", "Yody"],
         ["Váy liền", "Áo kiểu nữ", "Chân váy", "Quần âu nữ"], (150_000, 1_500_000)),
        ("Giày dép", ["Biti's", "Nike", "Adidas", "Converse", "Vans"],
         ["Giày thể thao", "Dép quai hậu", "Giày chạy bộ"], (250_000, 3_500_000)),
    ]),
    ("Nhà cửa & Đời sống", [
        ("Đồ gia dụng", ["Sunhouse", "Philips", "Panasonic", "Kangaroo"],
         ["Nồi cơm điện", "Máy xay sinh tố", "Nồi chiên không dầu", "Ấm siêu tốc"], (250_000, 4_000_000)),
        ("Nhà bếp", ["Lock&Lock", "Sunhouse", "Elmich"],
         ["Bộ nồi inox", "Chảo chống dính", "Hộp đựng thực phẩm"], (90_000, 2_500_000)),
        ("Trang trí nhà cửa", ["IKEA", "Rạng Đông", "Decor House"],
         ["Đèn ngủ", "Thảm trải sàn", "Kệ sách"], (100_000, 1_500_000)),
    ]),
    ("Sắc đẹp & Sức khỏe", [
        ("Chăm sóc da", ["La Roche-Posay", "Cocoon", "Innisfree", "Some By Mi"],
         ["Sữa rửa mặt", "Kem chống nắng", "Serum dưỡng da", "Toner"], (120_000, 900_000)),
        ("Trang điểm", ["Maybelline", "L'Oréal", "3CE", "Black Rouge"],
         ["Son kem lì", "Phấn nước cushion", "Mascara"], (120_000, 700_000)),
    ]),
    ("Sách & Văn phòng phẩm", [
        ("Sách", ["NXB Trẻ", "Nhã Nam", "Alpha Books", "NXB Kim Đồng"],
         ["Sách kỹ năng", "Tiểu thuyết", "Sách thiếu nhi", "Sách kinh tế"], (60_000, 350_000)),
        ("Văn phòng phẩm", ["Thiên Long", "Deli", "Hồng Hà"],
         ["Bút bi", "Sổ tay", "Bộ bút màu"], (15_000, 250_000)),
    ]),
    ("Thể thao & Dã ngoại", [
        ("Dụng cụ thể thao", ["Adidas", "Nike", "Kamito", "Yonex"],
         ["Vợt cầu lông", "Bóng đá", "Thảm tập yoga", "Tạ tay"], (80_000, 3_000_000)),
        ("Đồ dã ngoại", ["Naturehike", "Coleman"],
         ["Lều cắm trại", "Ghế xếp dã ngoại", "Bình giữ nhiệt"], (150_000, 4_000_000)),
    ]),
]

MODEL_SUFFIXES = ["", " Pro", " Plus", " Lite", " Max", " Mini", " SE"]

PAYMENT_METHODS = ["COD", "BANK_TRANSFER", "CREDIT_CARD", "E_WALLET"]
