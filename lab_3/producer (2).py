import sys
import json
import csv
from pathlib import Path
from PyQt6.QtWidgets import (
    QApplication, QWidget, QLabel, QLineEdit,
    QTextEdit, QPushButton, QVBoxLayout, QFileDialog
)
from kafka import KafkaProducer


# ===== Kafka config =====
BOOTSTRAP_SERVERS = "141.105.71.190:9092"


class ProducerApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL Producer (Kafka → PostgreSQL)")
        self.setGeometry(300, 300, 500, 500)

        self.producer = None  # lazy init

        layout = QVBoxLayout()

        self.table_input = QLineEdit()
        self.table_input.setPlaceholderText("Название таблицы (будет именем топика)")

        self.columns_input = QLineEdit()
        self.columns_input.setPlaceholderText("Столбцы: id,name,email")

        self.data_input = QTextEdit()
        self.data_input.setPlaceholderText(
            "Данные:\n1,Alex,a@mail.com\n2,Bob,b@mail.com"
        )

        self.send_button = QPushButton("Отправить в Kafka")
        self.send_button.clicked.connect(self.send_manual)

        self.csv_button = QPushButton("Загрузить CSV")
        self.csv_button.clicked.connect(self.load_csv)

        self.json_button = QPushButton("Загрузить JSON")
        self.json_button.clicked.connect(self.load_json)

        self.status = QLabel("Статус: ожидание")

        for w in [
            self.table_input,
            self.columns_input,
            self.data_input,
            self.send_button,
            self.csv_button,
            self.json_button,
            self.status
        ]:
            layout.addWidget(w)

        self.setLayout(layout)

    # ===== Lazy Kafka Producer =====
    def get_producer(self):
        if self.producer is None:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    key_serializer=lambda v: v.encode("utf-8"),
                    acks="all",
                    retries=5,
                    linger_ms=10
                )
            except Exception as e:
                self.status.setText("❌ Kafka недоступна")
                print(e)
                return None
        return self.producer

    # ===== Validation =====
    def validate(self, table, columns, rows):
        if not table:
            return "Название таблицы пустое"
        if not columns:
            return "Не указаны столбцы"
        for i, row in enumerate(rows):
            if len(row) != len(columns):
                return f"Строка {i+1}: количество значений не совпадает со столбцами"
        return None

    # ===== Manual Input =====
    def send_manual(self):
        table = self.table_input.text().strip()
        columns = [c.strip() for c in self.columns_input.text().split(",") if c.strip()]

        rows = []
        for line in self.data_input.toPlainText().splitlines():
            if line.strip():
                rows.append([v.strip() for v in line.split(",")])

        error = self.validate(table, columns, rows)
        if error:
            self.status.setText(f"❌ {error}")
            return

        self.send_to_kafka(table, columns, rows, "gui")

    # ===== CSV =====
    def load_csv(self):
        path, _ = QFileDialog.getOpenFileName(self, "CSV файл", "", "CSV (*.csv)")
        if not path:
            return

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            columns = next(reader)
            rows = list(reader)

        table = Path(path).stem  # имя файла = имя таблицы = имя топика
        self.send_to_kafka(table, columns, rows, "csv")

    # ===== JSON =====
    def load_json(self):
        path, _ = QFileDialog.getOpenFileName(self, "JSON файл", "", "JSON (*.json)")
        if not path:
            return

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        self.send_to_kafka(
            data["table"],
            data["columns"],
            data["rows"],
            "json"
        )

    # ===== Send =====
    def send_to_kafka(self, table, columns, rows, source):
        producer = self.get_producer()
        if producer is None:
            return

        # topic = table  (Kafka auto-create topic)
        topic = table

        message = {
            "schema_version": 1,
            "table": table,
            "columns": columns,
            "rows": rows,
            "source": source,
            "meta": {
                "producer": "pyqt-gui",
                "topic": topic
            }
        }

        try:
            producer.send(
                'auto-topic',           # 👈 динамический топик
                key=table,             # ключ = имя таблицы
                value=message
            )
            producer.flush()
            self.status.setText(f"✅ Отправлено в topic '{topic}'")
        except Exception as e:
            self.status.setText("❌ Ошибка отправки")
            print(e)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = ProducerApp()
    win.show()
    sys.exit(app.exec())
