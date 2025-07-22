import sys
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QTableWidget,
    QTableWidgetItem,
    QPushButton,
    QStatusBar,
    QGroupBox,
    QTabWidget,
    QComboBox,
)
from PySide6.QtCore import QTimer, QThread
from kafka_client import KafkaConsumerWorker
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import logging
import numpy as np
import time # Added for time.time()

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

class AnomalyDetectionGUI(QMainWindow):
    def __init__(self):
        super().__init__()

        self.start_time = time.time() # Store start time

        self.setWindowTitle("Pakistan Stock Exchange Anomaly Detector")
        self.setGeometry(100, 100, 1200, 800)

        # Main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)

        # Top section with controls and charts
        top_section_layout = QHBoxLayout()
        main_layout.addLayout(top_section_layout)

        # Control Panel
        control_panel_group = QGroupBox("Controls")
        control_panel_layout = QVBoxLayout()
        self.connect_button = QPushButton("Connect")
        self.disconnect_button = QPushButton("Disconnect")
        self.disconnect_button.setEnabled(False)
        control_panel_layout.addWidget(self.connect_button)
        control_panel_layout.addWidget(self.disconnect_button)
        
        # Stock selection dropdown
        self.stock_selector = QComboBox()
        self.stock_selector.addItem("All Stocks")
        control_panel_layout.addWidget(self.stock_selector)
        
        control_panel_layout.addStretch()
        control_panel_group.setLayout(control_panel_layout)
        top_section_layout.addWidget(control_panel_group)

        # Charts
        self.tab_widget = QTabWidget()
        top_section_layout.addWidget(self.tab_widget, stretch=1)

        self.stock_chart_canvas, self.stock_chart_ax = self.create_plot("Stock Data")
        self.tab_widget.addTab(self.stock_chart_canvas, "Stocks")

        # Economic Indicators Tab
        self.econ_tab = QWidget()
        self.econ_layout = QGridLayout(self.econ_tab)
        self.econ_table = QTableWidget(3, 2)
        self.econ_table.setHorizontalHeaderLabels(["Indicator", "Value"])
        self.econ_table.verticalHeader().setVisible(False)
        self.econ_table.horizontalHeader().setStretchLastSection(True)
        self.econ_table.setItem(0, 0, QTableWidgetItem("Exchange Rate (USD/PKR)"))
        self.econ_table.setItem(1, 0, QTableWidgetItem("Inflation Rate (%)"))
        self.econ_table.setItem(2, 0, QTableWidgetItem("Interest Rate (%)"))
        self.econ_layout.addWidget(self.econ_table)
        self.tab_widget.addTab(self.econ_tab, "Economic Indicators")

        # News Sentiment Tab
        self.sentiment_tab = QWidget()
        self.sentiment_layout = QVBoxLayout(self.sentiment_tab)
        self.sentiment_headline = QTableWidget(1, 2)
        self.sentiment_headline.setHorizontalHeaderLabels(["Headline", "Sentiment"])
        self.sentiment_headline.verticalHeader().setVisible(False)
        self.sentiment_headline.horizontalHeader().setStretchLastSection(True)
        self.sentiment_layout.addWidget(self.sentiment_headline)
        self.tab_widget.addTab(self.sentiment_tab, "News Sentiment")

        # Anomaly Log
        anomaly_group = QGroupBox("Anomaly Log")
        anomaly_layout = QVBoxLayout()
        self.anomaly_table = QTableWidget(1, 5) # Start with one row for headers
        self.anomaly_table.setHorizontalHeaderLabels(
            ["Timestamp", "Topic", "Key", "Value", "Anomaly Score"]
        )
        self.anomaly_table.horizontalHeader().setStretchLastSection(True)
        anomaly_layout.addWidget(self.anomaly_table)
        anomaly_group.setLayout(anomaly_layout)
        main_layout.addWidget(anomaly_group, stretch=1)

        # Status Bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready. Please connect to Kafka.")

        

        # Data store for charts
        self.stock_data = {}
        self.displayed_stock = "All Stocks" # New attribute to store the currently displayed stock

        # Kafka Integration
        self.kafka_thread = None
        self.kafka_worker = None
        self.connect_button.clicked.connect(self.start_kafka_consumer)
        self.disconnect_button.clicked.connect(self.stop_kafka_consumer)
        self.stock_selector.currentIndexChanged.connect(self.update_stock_chart_display)

    def start_kafka_consumer(self):
        self.kafka_thread = QThread()
        self.kafka_worker = KafkaConsumerWorker()
        self.kafka_worker.moveToThread(self.kafka_thread)

        # Connect signals to slots
        self.kafka_worker.stockDataReceived.connect(self.update_stock_chart)
        self.kafka_worker.econDataReceived.connect(self.update_econ_table)
        self.kafka_worker.sentimentDataReceived.connect(self.update_sentiment_table)
        self.kafka_worker.anomalyReceived.connect(self.update_anomaly_log)
        self.kafka_worker.connectionStatus.connect(self.update_status_bar)

        self.kafka_thread.started.connect(self.kafka_worker.run)
        self.kafka_thread.start()

        self.connect_button.setEnabled(False)
        self.disconnect_button.setEnabled(True)
        self.status_bar.showMessage("Connecting to Kafka...")

    def stop_kafka_consumer(self):
        if self.kafka_worker:
            self.kafka_worker.stop()
        if self.kafka_thread:
            self.kafka_thread.quit()
            self.kafka_thread.wait()

        self.connect_button.setEnabled(True)
        self.disconnect_button.setEnabled(False)

    def update_stock_chart_display(self):
        self.displayed_stock = self.stock_selector.currentText()
        self.update_stock_chart(None) # Trigger a redraw with the new selection

    def update_status_bar(self, message):
        self.status_bar.showMessage(message)

    def update_stock_chart(self, data):
        if data: # Only process new data if it's not a redraw trigger
            logger.info(f"update_stock_chart: Received data for {data.get('symbol')}")
            symbol = data.get('symbol')
            price = data.get('price')
            timestamp = data.get('timestamp')

            if not symbol or price is None or timestamp is None:
                return

            if symbol not in self.stock_data:
                self.stock_data[symbol] = {'x': [], 'y': []}
                # Add new stock to dropdown if it's not already there
                if self.stock_selector.findText(symbol) == -1:
                    self.stock_selector.addItem(symbol)

            elapsed_time = time.time() - self.start_time
            self.stock_data[symbol]['x'].append(elapsed_time)
            self.stock_data[symbol]['y'].append(price)

            # Keep only the last 100 data points for performance
            for s in self.stock_data:
                self.stock_data[s]['x'] = self.stock_data[s]['x'][-100:]
                self.stock_data[s]['y'] = self.stock_data[s]['y'][-100:]

        self.stock_chart_ax.clear()
        self.stock_chart_ax.set_title("Stock Data")
        self.stock_chart_ax.set_xlabel("Time (seconds)")
        self.stock_chart_ax.set_ylabel("Value")
        self.stock_chart_ax.grid(True)
        # Plot based on selection
        if self.displayed_stock == "All Stocks":
            for s, data_points in self.stock_data.items():
                self.stock_chart_ax.plot(data_points['x'], data_points['y'], label=s)
                logger.info(f"Plotting {s}: x_len={len(data_points['x'])}, y_len={len(data_points['y'])}")
            self.stock_chart_ax.autoscale_view(True, True, True)
        else:
            s = self.displayed_stock
            if s in self.stock_data:
                data_points = self.stock_data[s]
                self.stock_chart_ax.plot(data_points['x'], data_points['y'], label=s)
                
                # Dynamic Y-axis scaling for individual stocks
                if data_points['y']:
                    min_val = min(data_points['y'])
                    max_val = max(data_points['y'])
                    
                    if min_val == max_val:
                        # If there's no fluctuation, add a small, fixed margin
                        margin = 1.0
                        self.stock_chart_ax.set_ylim(min_val - margin, max_val + margin)
                    else:
                        # Otherwise, add a margin based on the data's range
                        data_range = max_val - min_val
                        margin = data_range * 0.1  # 10% padding
                        self.stock_chart_ax.set_ylim(min_val - margin, max_val + margin)
                logger.info(f"Plotting {s}: x_len={len(data_points['x'])}, y_len={len(data_points['y'])}")

        self.stock_chart_ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
        self.stock_chart_canvas.draw()

    def update_econ_table(self, data):
        indicator = data.get('key')
        value = data.get('rate') or data.get('USD_PKR')
        if indicator and value is not None:
            for row in range(self.econ_table.rowCount()):
                item = self.econ_table.item(row, 0)
                if item and item.text().lower().startswith(indicator.replace('_rate', '').replace('_', ' ')): # Corrected
                    self.econ_table.setItem(row, 1, QTableWidgetItem(str(value)))
                    break

    def update_sentiment_table(self, data):
        headline = data.get('headline')
        sentiment = data.get('sentiment')
        if headline and sentiment:
            self.sentiment_headline.setItem(0, 0, QTableWidgetItem(headline))
            self.sentiment_headline.setItem(0, 1, QTableWidgetItem(sentiment.capitalize()))

    def update_anomaly_log(self, data):
        row_position = self.anomaly_table.rowCount()
        self.anomaly_table.insertRow(row_position)

        timestamp = data.get('timestamp', '')
        symbol = data.get('symbol', '')
        price_change = data.get('price_change', '')
        current_price = data.get('current_price', '')
        last_price = data.get('last_price', '')

        self.anomaly_table.setItem(row_position, 0, QTableWidgetItem(str(timestamp)))
        self.anomaly_table.setItem(row_position, 1, QTableWidgetItem("Stock Price Anomaly"))
        self.anomaly_table.setItem(row_position, 2, QTableWidgetItem(symbol))
        self.anomaly_table.setItem(row_position, 3, QTableWidgetItem(f"Price changed by {price_change:.2f} from {last_price} to {current_price}"))
        self.anomaly_table.setItem(row_position, 4, QTableWidgetItem("N/A"))

        self.anomaly_table.scrollToBottom()

    

    def create_plot(self, title):
        fig = Figure(constrained_layout=True)
        canvas = FigureCanvas(fig)
        ax = fig.add_subplot(111)
        ax.set_title(title)
        ax.set_xlabel("Time (seconds)")
        ax.set_ylabel("Value")
        ax.grid(True)
        ax.autoscale_view(scalex=True, scaley=True) # Auto-scale X and Y
        return canvas, ax

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = AnomalyDetectionGUI()
    window.show()
    sys.exit(app.exec())