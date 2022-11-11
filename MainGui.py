import sys

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QCalendarWidget, QLabel, QFileDialog, QMessageBox

from ExtraClasses import Tools
from Retriever import get_cols, get_uniques, get_data, out_res
from Scraper import thread_scrape

app = QtWidgets.QApplication(sys.argv)
MainWind = QtWidgets.QMainWindow()


class Ui_MainWindow(QWidget):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(418, 500)
        MainWidget = QtWidgets.QWidget(MainWindow)

        nTools = Tools()
        font = nTools.font_format()

        self.Start_Button = QtWidgets.QPushButton(MainWidget)
        self.Start_Button.setGeometry(QtCore.QRect(120, 50, 165, 29))
        self.Start_Button.setFont(font)

        self.End_Button = QtWidgets.QPushButton(MainWidget)
        self.End_Button.setGeometry(QtCore.QRect(120, 100, 165, 29))
        self.End_Button.setFont(font)

        self.Run_Button = QtWidgets.QPushButton(MainWidget)
        self.Run_Button.setGeometry(QtCore.QRect(120, 180, 165, 29))
        self.Run_Button.setFont(font)

        self.Combo_Box = QtWidgets.QComboBox(MainWidget)
        self.Combo_Box.setGeometry(QtCore.QRect(120, 220, 165, 29))
        self.Combo_Box.setFont(font)

        for i in range(2, 26, 2):
            self.Combo_Box.addItem(str(i))

        self.CSV_Button = QtWidgets.QPushButton(MainWidget)
        self.CSV_Button.setGeometry(QtCore.QRect(120, 300, 165, 29))
        self.CSV_Button.setFont(font)

        self.Uniques_Button = QtWidgets.QPushButton(MainWidget)
        self.Uniques_Button.setGeometry(QtCore.QRect(120, 340, 165, 29))
        self.Uniques_Button.setFont(font)

        self.Extr_Button = QtWidgets.QPushButton(MainWidget)
        self.Extr_Button.setGeometry(QtCore.QRect(120, 380, 165, 29))
        self.Extr_Button.setFont(font)

        MainWindow.setCentralWidget(MainWidget)

        MainWindow.setWindowTitle("Main Window")
        self.Start_Button.setText("Start Date")
        self.End_Button.setText("End Date")
        self.Run_Button.setText("Scrape")
        self.CSV_Button.setText("Get Cols")
        self.Uniques_Button.setText("Get Uniques")
        self.Extr_Button.setText("Extract By CSV")

        self.Start_Button.clicked.connect(self.choose_sdate)
        self.End_Button.clicked.connect(self.choose_edate)
        self.Run_Button.clicked.connect(self.run_click)
        self.CSV_Button.clicked.connect(self.get_cols)
        self.Uniques_Button.clicked.connect(self.get_unqs)
        self.Extr_Button.clicked.connect(self.get_all)

        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def choose_sdate(self):
        self.sCal = Calendar()
        self.nCal = self.sCal.initUI(0)

    def choose_edate(self):
        self.sCal = Calendar()
        self.nCal = self.sCal.initUI(1)

    def run_click(self):
        nTool = Tools()
        start_date = str(self.Start_Button.text())
        end_date = str(self.End_Button.text())

        if nTool.check_dates(start_date, end_date):
            self.msgBox('!', 'Error')
            return

        threads = int(self.Combo_Box.currentText())
        thread_scrape(start_date, end_date, threads)

        self.msgBox('!', 'Done')


    def get_cols(self):
        try:
            file_name = self.saveFileDialog()
            file_name = file_name + '.csv'
            df = get_cols()
            df.to_csv(file_name,  encoding='utf-8-sig', index=False)
            self.msgBox('!', 'Done')
        except:
            pass

    def get_unqs(self):
        try:
            nTool = Tools()
            file_name = self.saveFileDialog()
            file_name = file_name + '.csv'

            start_date = str(self.Start_Button.text())
            end_date = str(self.End_Button.text())

            if nTool.check_dates(start_date, end_date):
                self.msgBox('!', 'Error')
                return

            df = get_uniques(start_date, end_date)

            df.to_csv(file_name,  encoding='utf-8-sig', index=False)
            self.msgBox('!', 'Done')
        except:
            print('Connection Issue')
            pass

    def get_all(self):
        nTool = Tools()

        start_date = str(self.Start_Button.text())
        end_date = str(self.End_Button.text())

        if nTool.check_dates(start_date, end_date):
            self.msgBox('!', 'Error')
            return

        file_name = self.openFileNameDialog()
        if len(file_name) == 0:
            return

        save_name = self.saveFileDialog()
        if len(save_name) == 0:
            return

        df = get_data(file_name, start_date, end_date)

        out_res(df, save_name)

        self.msgBox('!', 'Done')


    def openFileNameDialog(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        fileName, _ = QFileDialog.getOpenFileName(self, "QFileDialog.getOpenFileName()", "",
                                                  "CSV Files (*.csv)", options=options)
        return fileName

    def saveFileDialog(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        fileName, _ = QFileDialog.getSaveFileName(self, "QFileDialog.getSaveFileName()", "",
                                                  "", options=options)
        return fileName

    def msgBox(self, title, text):
        QMessageBox.about(self, title, text)

class Calendar(QWidget):

    def __init__(self):
        super().__init__()

    def initUI(self, button_n):
        self.button_n = button_n
        self.hidden = True
        self.nTool = Tools()

        vbox = QVBoxLayout(self)

        self.cal = QCalendarWidget(self)
        self.cal.setGridVisible(True)

        self.setObjectName("CalendarWindow")

        vbox.addWidget(self.cal)

        self.lbl = QLabel(self)

        self.Choose_Date = QtWidgets.QPushButton()
        self.Choose_Date.setGeometry(QtCore.QRect(120, 310, 165, 29))
        font = self.nTool.font_format()

        self.Choose_Date.setFont(font)
        self.Choose_Date.setObjectName("Choose_Date")
        self.Choose_Date.setText("Choose Date")

        self.Choose_Date.clicked.connect(self.Date_Pick)

        vbox.addWidget(self.Choose_Date)
        self.setLayout(vbox)

        self.setGeometry(300, 300, 350, 300)
        self.setWindowTitle('Calendar')
        self.show()

    def Date_Pick(self):
        if self.button_n == 0:
            self.Start_Pick()

        if self.button_n == 1:
            self.End_Pick()

    def Start_Pick(self):
        choose_date = str(self.nTool.qtDate(self.cal.selectedDate().toPyDate()))
        ui.Start_Button.setText(choose_date)
        self.hide()

    def End_Pick(self):
        choose_date = str(self.nTool.qtDate(self.cal.selectedDate().toPyDate()))
        ui.End_Button.setText(choose_date)
        self.hide()


def Main_Page():
    global ui
    ui = Ui_MainWindow()
    ui.setupUi(MainWind)
    MainWind.show()
    sys.exit(app.exec_())


def main():
    Main_Page()


if __name__ == "__main__":
    main()
