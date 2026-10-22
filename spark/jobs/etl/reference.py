"""Reference / conformance data the ETL applies on top of the OLTP data."""

UNKNOWN_KEY = -1
UNKNOWN_LABEL = "Không xác định"

# Free-text city input -> canonical name (keys are lower-cased)
CITY_ALIASES = {
    "tp hcm": "TP. Hồ Chí Minh", "tp.hcm": "TP. Hồ Chí Minh", "hcm": "TP. Hồ Chí Minh",
    "hồ chí minh": "TP. Hồ Chí Minh", "sài gòn": "TP. Hồ Chí Minh",
    "ha noi": "Hà Nội", "hn": "Hà Nội", "tp. hà nội": "Hà Nội",
    "da nang": "Đà Nẵng", "hai phong": "Hải Phòng", "can tho": "Cần Thơ",
}

CITY_REGION = {
    "Hà Nội": "Miền Bắc", "Hải Phòng": "Miền Bắc", "Quảng Ninh": "Miền Bắc", "Bắc Ninh": "Miền Bắc",
    "Thái Nguyên": "Miền Bắc", "Ninh Bình": "Miền Bắc",
    "Thanh Hóa": "Miền Trung", "Nghệ An": "Miền Trung", "Huế": "Miền Trung", "Đà Nẵng": "Miền Trung",
    "Khánh Hòa": "Miền Trung", "Gia Lai": "Miền Trung", "Đắk Lắk": "Miền Trung", "Lâm Đồng": "Miền Trung",
    "TP. Hồ Chí Minh": "Miền Nam", "Đồng Nai": "Miền Nam", "Tây Ninh": "Miền Nam", "Cần Thơ": "Miền Nam",
    "An Giang": "Miền Nam", "Vĩnh Long": "Miền Nam",
}

GENDERS = {"M": "Nam", "F": "Nữ"}

# (upper bound exclusive, label); None = no upper bound
PRICE_TIERS = [(200_000, "Bình dân"), (2_000_000, "Trung cấp"), (10_000_000, "Cao cấp"), (None, "Hạng sang")]

DAY_NAMES = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
HOLIDAYS = {  # fixed-date national holidays (MM-dd)
    "01-01": "Tết Dương lịch",
    "04-30": "Ngày Giải phóng miền Nam",
    "05-01": "Quốc tế Lao động",
    "09-02": "Quốc khánh",
}

# key, code, name, channel
PAYMENT_METHODS = [
    (1, "COD", "Thanh toán khi nhận hàng", "Offline"),
    (2, "BANK_TRANSFER", "Chuyển khoản ngân hàng", "Online"),
    (3, "CREDIT_CARD", "Thẻ tín dụng / ghi nợ", "Online"),
    (4, "E_WALLET", "Ví điện tử", "Online"),
]

# key, code, name, group, is_final
ORDER_STATUSES = [
    (1, "PENDING", "Chờ xác nhận", "Đang xử lý", False),
    (2, "CONFIRMED", "Đã xác nhận", "Đang xử lý", False),
    (3, "SHIPPING", "Đang giao hàng", "Đang xử lý", False),
    (4, "DELIVERED", "Đã giao hàng", "Thành công", True),
    (5, "CANCELLED", "Đã hủy", "Hủy / Trả hàng", True),
    (6, "RETURNED", "Đã trả hàng", "Hủy / Trả hàng", True),
]
