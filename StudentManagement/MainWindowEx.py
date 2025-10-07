import base64
import traceback
import os
import mysql.connector
from PyQt6.QtGui import QPixmap, QIcon
from PyQt6.QtWidgets import QTableWidgetItem, QFileDialog, QMessageBox
from MainWindow import Ui_MainWindow


class MainWindowEx(Ui_MainWindow):
    def __init__(self):
        super().__init__()
        self.default_avatar = "images/ic_no_avatar.png"
        self.conn = None
        self.avatar = None

    def setupUi(self, MainWindow):
        super().setupUi(MainWindow)
        self.MainWindow = MainWindow
        self.lineEditId.setReadOnly(True)
        # --- Sửa lỗi icon không hiện ---
        base_dir = os.path.dirname(__file__)
        img_dir = os.path.join(base_dir, "images")
        self.pushButtonNew.setIcon(QIcon(os.path.join(img_dir, "ic_new.png")))
        self.pushButtonInsert.setIcon(QIcon(os.path.join(img_dir, "ic_save.png")))
        self.pushButtonUpdate.setIcon(QIcon(os.path.join(img_dir, "ic_update.png")))
        self.pushButtonRemove.setIcon(QIcon(os.path.join(img_dir, "ic_delete.png")))
        self.labelAvatar.setPixmap(QPixmap(os.path.join(img_dir, "ic_no_avatar.png")))

        # --- Gắn sự kiện ---
        self.tableWidgetStudent.itemSelectionChanged.connect(self.processItemSelection)
        self.pushButtonAvatar.clicked.connect(self.pickAvatar)
        self.pushButtonRemoveAvatar.clicked.connect(self.removeAvatar)
        self.pushButtonInsert.clicked.connect(self.processInsert)
        self.pushButtonUpdate.clicked.connect(self.processUpdate)
        self.pushButtonRemove.clicked.connect(self.processRemove)
        self.pushButtonNew.clicked.connect(self.clearData)

    def show(self):
        self.MainWindow.show()

    # ---------- Kết nối MySQL ----------
    def connectMySQL(self):
        try:
            self.conn = mysql.connector.connect(
                host="localhost",
                port=3306,
                database="studentmanagement",
                user="Phuc_Diem",
                password="Huyndiem07@",
                autocommit=True
            )
            if self.conn.is_connected():
                pass
            else:
                QMessageBox.warning(self.MainWindow, "Cảnh báo", "Không thể kết nối tới MySQL.")
        except mysql.connector.Error as err:
            QMessageBox.critical(self.MainWindow, "Lỗi MySQL", f"Lỗi: {err}")

    # ---------- Hiển thị toàn bộ sinh viên ----------
    def selectAllStudent(self):
        if not self.conn or not self.conn.is_connected():
            return

        cursor = self.conn.cursor(buffered=True)
        cursor.execute("SELECT * FROM student")
        rows = cursor.fetchall()
        cursor.close()

        self.tableWidgetStudent.setRowCount(0)
        for item in rows:
            row = self.tableWidgetStudent.rowCount()
            self.tableWidgetStudent.insertRow(row)
            self.tableWidgetStudent.setItem(row, 0, QTableWidgetItem(str(item[0])))
            self.tableWidgetStudent.setItem(row, 1, QTableWidgetItem(item[1]))
            self.tableWidgetStudent.setItem(row, 2, QTableWidgetItem(item[2]))
            self.tableWidgetStudent.setItem(row, 3, QTableWidgetItem(str(item[3])))

    # ---------- Khi chọn sinh viên ----------
    def processItemSelection(self):
        try:
            row = self.tableWidgetStudent.currentRow()
            if row == -1:
                return

            code = self.tableWidgetStudent.item(row, 1).text()
            cursor = self.conn.cursor(buffered=True)
            cursor.execute("SELECT * FROM student WHERE Code=%s", (code,))
            item = cursor.fetchone()
            cursor.close()

            if item:
                self.lineEditId.setText(str(item[0]))
                self.lineEditCode.setText(item[1])
                self.lineEditName.setText(item[2])
                self.lineEditAge.setText(str(item[3]))
                self.lineEditIntro.setText(item[5] if item[5] else "")
                self.showAvatar(item[4])
        except Exception:
            traceback.print_exc()

    def showAvatar(self, avatar_blob):
        try:
            if avatar_blob:
                pixmap = QPixmap()
                if pixmap.loadFromData(base64.b64decode(avatar_blob)):
                    self.labelAvatar.setPixmap(pixmap)
                else:
                    self.labelAvatar.setPixmap(QPixmap(self.default_avatar))
            else:
                self.labelAvatar.setPixmap(QPixmap(self.default_avatar))
        except Exception:
            self.labelAvatar.setPixmap(QPixmap(self.default_avatar))

    # ---------- Chọn ảnh ----------
    def pickAvatar(self):
        filename, _ = QFileDialog.getOpenFileName(
            self.MainWindow,
            "Chọn ảnh đại diện",
            "",
            "Hình ảnh (*.png *.jpg *.jpeg *.bmp);;Tất cả tệp (*)"
        )
        if not filename:
            return

        pixmap = QPixmap(filename)
        self.labelAvatar.setPixmap(pixmap)
        with open(filename, "rb") as file:
            self.avatar = base64.b64encode(file.read())

    # ---------- Xoá ảnh ----------
    def removeAvatar(self):
        self.avatar = None
        self.labelAvatar.setPixmap(QPixmap(self.default_avatar))

    # ---------- Thêm ----------
    def processInsert(self):
        try:
            cursor = self.conn.cursor(buffered=True)
            sql = """INSERT INTO student(Code, Name, Age, Avatar, Intro)
                     VALUES(%s, %s, %s, %s, %s)"""
            data = (
                self.lineEditCode.text(),
                self.lineEditName.text(),
                int(self.lineEditAge.text()),
                self.avatar,
                self.lineEditIntro.text()
            )
            cursor.execute(sql, data)
            cursor.close()
            self.selectAllStudent()
        except Exception:
            traceback.print_exc()

    # ---------- Cập nhật ----------
    def processUpdate(self):
        try:
            cursor = self.conn.cursor(buffered=True)
            sql = """UPDATE student
                     SET Code=%s, Name=%s, Age=%s, Avatar=%s, Intro=%s
                     WHERE ID=%s"""
            data = (
                self.lineEditCode.text(),
                self.lineEditName.text(),
                int(self.lineEditAge.text()),
                self.avatar,
                self.lineEditIntro.text(),
                int(self.lineEditId.text())
            )
            cursor.execute(sql, data)
            cursor.close()
            self.selectAllStudent()
        except Exception:
            traceback.print_exc()

    # ---------- Xoá ----------
    def processRemove(self):
        dlg = QMessageBox(self.MainWindow)
        dlg.setWindowTitle("Xác nhận xoá")
        dlg.setText("Bạn có chắc chắn muốn xoá sinh viên này?")
        dlg.setIcon(QMessageBox.Icon.Question)
        dlg.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if dlg.exec() == QMessageBox.StandardButton.No:
            return

        try:
            cursor = self.conn.cursor(buffered=True)
            cursor.execute("DELETE FROM student WHERE ID=%s", (self.lineEditId.text(),))
            cursor.close()
            self.selectAllStudent()
            self.clearData()
        except Exception:
            traceback.print_exc()

    # ---------- Làm mới ----------
    def clearData(self):
        self.lineEditId.clear()
        self.lineEditCode.clear()
        self.lineEditName.clear()
        self.lineEditAge.clear()
        self.lineEditIntro.clear()
        self.avatar = None
        self.labelAvatar.setPixmap(QPixmap(self.default_avatar))