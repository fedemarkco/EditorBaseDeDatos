from logging import exception
from PyQt5 import QtCore, QtWidgets, QtGui
from sshtunnel import SSHTunnelForwarder

import ibm_db_dbi as db
import mysql.connector
import happybase
import cx_Oracle
import requests
import pyodbc
import json
import sys
import re
import os

# Main class that will display the graphical interface
class Main(QtWidgets.QWidget):
  def __init__(self, parent=None):
    super(Main, self).__init__()
    self.setWindowTitle("Database")
    self.setGeometry(300, 300, 1000, 530)
    self.setStyleSheet("background-color: #414954;")
    self.setWindowFlags(self.windowFlags() | QtCore.Qt.MSWindowsFixedSizeDialogHint)

    self.sshEnabled = False
    self.sshHandle = None
    self.isConnect = False
    self.database = None
    self.dbCon = ""
    self.quer = ""

    self.tab = QtWidgets.QTabWidget()
    self.tab.setStyleSheet("background-color: white; border-width: 0px; color: black;")
    self.tab.setFixedHeight(430)

    exitButton = QtWidgets.QPushButton("Exit")
    exitButton.setStyleSheet("background-color: white;")
    exitButton.setFixedWidth(150)
    exitButton.setFixedHeight(50)
    exitButton.setFont(QtGui.QFont("Arial",12,QtGui.QFont.Bold))
    exitButton.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))

    mLeft = menuLeft()
    self.tabQuery = tabQuery(self)
    self.tabResponse = tabResponseError(self)

    self.tab.addTab(self.tabQuery, 'Query')
    self.tab.addTab(self.tabResponse, 'Response')
    self.tab.setFont(QtGui.QFont("Arial",12,QtGui.QFont.Bold))

    exitButton.clicked.connect(self.close)
    mLeft.DBbutton.clicked.connect(self.openWinDB)

    grid = QtWidgets.QGridLayout()
    grid.addWidget(mLeft.DBbutton, 0, 0, alignment=QtCore.Qt.AlignTop)
    grid.addWidget(self.tab, 0, 1, alignment=QtCore.Qt.AlignTop)
    grid.addWidget(exitButton, 1, 1, alignment=QtCore.Qt.AlignCenter | QtCore.Qt.AlignLeft)
    grid.setSpacing(0)
    grid.setContentsMargins(0, 10, 0, 0)

    self.setLayout(grid)

  def openWinDB(self):
    WinDatabase(self).exec_()

# Generate the left menu for the DATABASES button
class menuLeft(QtWidgets.QWidget):
  def __init__(self, parent=None):
    super(menuLeft, self).__init__(parent)

    font = QtGui.QFont("Arial",12,QtGui.QFont.Bold)

    self.DBbutton = QtWidgets.QPushButton("DATABASES")
    self.DBbutton.setFont(font)
    self.DBbutton.setStyleSheet("color: #c1d0d8;")
    self.DBbutton.setFixedWidth(150)
    self.DBbutton.setFixedHeight(50)

    self.DBbutton.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))

# Generate the Query tab
class tabQuery(QtWidgets.QWidget):

  def __init__(self, parent=None):
    super(tabQuery, self).__init__(parent)

    self.parent = parent

    self.setStyleSheet("background-color: white;")

    font = QtGui.QFont("Arial", 12)

    self.textboxQuery = QtWidgets.QPlainTextEdit(self)
    self.textboxQuery.setFont(font)
    self.textboxQuery.move(0, 30)
    self.textboxQuery.resize(849, 320)
    self.textboxQuery.setReadOnly(True)

    executeButton = QtWidgets.QPushButton("Execute")
    executeButton.setFixedHeight(30)
    executeButton.setFixedWidth(80)
    executeButton.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
    executeButton.setStyleSheet("background-color: #4bce69; color: white;")
    executeButton.setFont(font)

    self.hbox = QtWidgets.QVBoxLayout()
    self.hbox.addWidget(self.textboxQuery)
    self.hbox.addWidget(executeButton, alignment=QtCore.Qt.AlignRight)

    self.setLayout(self.hbox)

    executeButton.clicked.connect(self.runQuery)

  # Method that will allow running database queries
  def runQuery(self):
    q = self.textboxQuery.toPlainText().strip()
    if q != "":
      if self.parent.parent.isConnect:
        if self.parent.parent.dbCon == "mysql":
          cols, rows = self.parent.parent.database.mysqlQuery(q)
          if cols is None and rows is None:
            self.tabQuery = tabResponseError("Empty")
          else:
            if cols == "Error" and rows == "Error":
              self.tabQuery = tabResponseError("Query Error")
            else:
              self.tabQuery = tabDatabaseResponse(cols, rows)
          self.parent.parent.tab.removeTab(1)
          scrollbar = QtWidgets.QScrollArea()
          scrollbar.setWidget(self.tabQuery)
          scrollbar.setWidgetResizable(True)
          self.parent.parent.tab.addTab(scrollbar, 'Response')

        if self.parent.parent.dbCon == "sqlserver":
          cols, rows = self.parent.parent.database.sqlServerQuery(q)
          if cols is None and rows is None:
            self.tabQuery = tabResponseError("Empty")
          else:
            if cols == "Error" and rows == "Error":
              self.tabQuery = tabResponseError("Query Error")
            else:
              self.tabQuery = tabDatabaseResponse(cols, rows)
          self.parent.parent.tab.removeTab(1)
          scrollbar = QtWidgets.QScrollArea()
          scrollbar.setWidget(self.tabQuery)
          scrollbar.setWidgetResizable(True)
          self.parent.parent.tab.addTab(scrollbar, 'Response')

        if self.parent.parent.dbCon == "oracle":
          cols, rows = self.parent.parent.database.oracleQuery(q)
          if cols is None and rows is None:
            self.tabQuery = tabResponseError("Empty")
          else:
            if cols == "Error" and rows == "Error":
              self.tabQuery = tabResponseError("Query Error")
            else:
              self.tabQuery = tabDatabaseResponse(cols, rows)
          self.parent.parent.tab.removeTab(1)
          scrollbar = QtWidgets.QScrollArea()
          scrollbar.setWidget(self.tabQuery)
          scrollbar.setWidgetResizable(True)
          self.parent.parent.tab.addTab(scrollbar, 'Response')

        if self.parent.parent.dbCon == "elasticsearch":
          resp = self.parent.parent.database.elasticQuery(q)
          if resp == "Error":
            self.tabQuery = tabResponseError("Query Error")
          else:
            self.tabQuery = tabResponseError(resp)
          self.parent.parent.tab.removeTab(1)
          self.parent.parent.tab.addTab(self.tabQuery, 'Response')

        if self.parent.parent.dbCon == "db2":
          cols, rows = self.parent.parent.database.db2Query(q)
          if cols is None and rows is None:
            self.tabQuery = tabResponseError("Empty")
          else:
            if cols == "Error" and rows == "Error":
              self.tabQuery = tabResponseError("Query Error")
            else:
              self.tabQuery = tabDatabaseResponse(cols, rows)
          self.parent.parent.tab.removeTab(1)
          scrollbar = QtWidgets.QScrollArea()
          scrollbar.setWidget(self.tabQuery)
          scrollbar.setWidgetResizable(True)
          self.parent.parent.tab.addTab(scrollbar, 'Response')

        if self.parent.parent.dbCon == "aws neptune":
          resp = self.parent.parent.database.AWSNeptuneQuery(q)
          if resp == "Error":
            self.tabQuery = tabResponseError("Query Error")
          else:
            self.tabQuery = tabResponseError(resp)
          self.parent.parent.tab.removeTab(1)
          self.parent.parent.tab.addTab(self.tabQuery, 'Response')

# Generate the Query tab
class tabResponseError(QtWidgets.QWidget):
  def __init__(self, text, parent=None):
    super(tabResponseError, self).__init__(parent)

    self.setStyleSheet("background-color: white;")

    self.text = text

    font = QtGui.QFont("Courier New", 12)

    self.textboxQuery = QtWidgets.QPlainTextEdit(text)
    self.textboxQuery.setFont(font)
    self.textboxQuery.move(0, 30)
    self.textboxQuery.resize(849, 320)
    self.textboxQuery.setReadOnly(True)

    self.hbox = QtWidgets.QVBoxLayout()
    self.hbox.addWidget(self.textboxQuery)

    self.setLayout(self.hbox)

# Generates the dropdown menu, showing the databases
class WinDatabase(QtWidgets.QDialog):
  def __init__(self, parent=None):
    super(WinDatabase, self).__init__(parent)

    self.parent = parent

    self.setFixedSize(300, 130)
    self.setWindowTitle("Configuration")
    self.setStyleSheet("background-color: white; color: black;")

    QBtn = QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel

    buttonBox = QtWidgets.QDialogButtonBox(QBtn)
    buttonBox.accepted.connect(self.accept)
    buttonBox.rejected.connect(self.reject)

    self.comboBox = QtWidgets.QComboBox()
    self.comboBox.addItems(
      [
        "Choose an option", 
        "MySql",
        "SqlServer",
        "Oracle",
        "ElasticSearch",
        "DB2",
        "Hbase",
        "AWS Neptune"
      ]
    )

    self.message = QtWidgets.QLabel("Select a database:")

    layout = QtWidgets.QGridLayout()
    layout.addWidget(self.message, 0, 0, alignment=QtCore.Qt.AlignTop)
    layout.addWidget(self.comboBox, 1, 0, alignment=QtCore.Qt.AlignTop)
    layout.addWidget(buttonBox, 2, 0, alignment=QtCore.Qt.AlignBottom)
    layout.setRowStretch(0, 1)
    layout.setRowStretch(1, 1)
    layout.setRowStretch(2, 3)

    self.setLayout(layout)

    self.comboBox.activated.connect(self.WinDatCon)

  # Method used to connect to databases
  def WinDatCon(self):
    if self.comboBox.currentText() == "Choose an option":
      return
    self.selectDatabase = showOptions(self)
    if self.selectDatabase.exec_() == QtWidgets.QDialog.Accepted:

      self.close()

      self.dbase = databases()

      self.parent.database = self.dbase
      self.parent.isConnect = None
      if self.parent.sshEnabled:
        self.parent.sshHandle.stop()
        self.parent.sshEnabled = False

      if self.comboBox.currentText().lower() == "mysql":
        try:
          host = self.selectDatabase.textboxHostMysql.text()
          port = self.selectDatabase.textboxPortMysql.text()
          user = str(self.selectDatabase.textboxUserMysql.text())
          passw = self.selectDatabase.textboxPasswMysql.text()
          db = self.selectDatabase.textboxDBMysql.text()
          self.parent.database.mysqlConnect(host, port, user, passw, db)
          self.parent.isConnect = True
          self.parent.dbCon = "mysql"
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setReadOnly(False)
          self.tabQuery.textboxQuery.setPlainText("")
          self.tabQuery.textboxQuery.setFocus()
        except:
          self.parent.isConnect = False
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setPlainText("Error conecting to the database")
          self.tabQuery.textboxQuery.setReadOnly(True)
          self.tabQuery.textboxQuery.setFocus()

      if self.comboBox.currentText().lower() == "sqlserver":
        try:
          server = self.selectDatabase.textboxServerSqlServer.text()
          port = self.selectDatabase.textboxPortSqlServer.text()
          user = self.selectDatabase.textboxUserSqlServer.text()
          passw = self.selectDatabase.textboxPasswSqlServer.text()
          db = self.selectDatabase.textboxDBSqlServer.text()
          self.parent.database.sqlServerConnect(server, port, user, passw, db)
          self.parent.isConnect = True
          self.parent.dbCon = "sqlserver"
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setReadOnly(False)
          self.tabQuery.textboxQuery.setPlainText("")
          self.tabQuery.textboxQuery.setFocus()
        except:
          self.parent.isConnect = False
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setPlainText("Error conecting to the database")
          self.tabQuery.textboxQuery.setReadOnly(True)
          self.tabQuery.textboxQuery.setFocus()

      if self.comboBox.currentText().lower() == "oracle":
        try:
          host = self.selectDatabase.textboxHostOracle.text()
          port = self.selectDatabase.textboxPortOracle.text()
          user = self.selectDatabase.textboxUserOracle.text()
          passw = self.selectDatabase.textboxPasswOracle.text()
          serviceName = self.selectDatabase.textboxSNOracle.text()
          self.parent.database.oracleConnect(host, user, passw, port, serviceName)
          self.parent.isConnect = True
          self.parent.dbCon = "oracle"
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setReadOnly(False)
          self.tabQuery.textboxQuery.setPlainText("")
          self.tabQuery.textboxQuery.setFocus()
        except:
          self.parent.isConnect = False
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setPlainText("Error conecting to the database")
          self.tabQuery.textboxQuery.setReadOnly(True)
          self.tabQuery.textboxQuery.setFocus()

      if self.comboBox.currentText().lower() == "elasticsearch":
        try:
          host = self.selectDatabase.textboxHostElasticSearch.text()
          port = self.selectDatabase.textboxPortElasticSearch.text()
          self.parent.database.elasticConnect(host, port)
          self.parent.isConnect = True
          self.parent.dbCon = "elasticsearch"
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setReadOnly(False)
          self.tabQuery.textboxQuery.setPlainText("")
          self.tabQuery.textboxQuery.setFocus()
        except:
          self.parent.isConnect = False
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setPlainText("Error conecting to the database")
          self.tabQuery.textboxQuery.setReadOnly(True)
          self.tabQuery.textboxQuery.setFocus()

      if self.comboBox.currentText().lower() == "db2":
        try:
          host = self.selectDatabase.textboxHostDB2.text()
          port = self.selectDatabase.textboxPortDB2.text()
          user = self.selectDatabase.textboxUserDB2.text()
          passw = self.selectDatabase.textboxPasswDB2.text()
          db = self.selectDatabase.textboxDBDB2.text()
          self.parent.database.db2Connect(host, port, user, passw, db)
          self.parent.isConnect = True
          self.parent.dbCon = "db2"
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setReadOnly(False)
          self.tabQuery.textboxQuery.setPlainText("")
          self.tabQuery.textboxQuery.setFocus()
        except:
          self.parent.isConnect = False
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setPlainText("Error conecting to the database")
          self.tabQuery.textboxQuery.setReadOnly(True)
          self.tabQuery.textboxQuery.setFocus()

      if self.comboBox.currentText().lower() == "aws neptune":
        try:
          sshAddress = self.selectDatabase.textboxSshAddressAWSNeptune.text()
          sshUsername = self.selectDatabase.textboxSshUserAWSNeptune.text()
          sshPkey = self.selectDatabase.textboxSshPkeyAWSNeptune.text()
          endpoint = self.selectDatabase.textboxEndpointAWSNeptune.text()
          port = int(self.selectDatabase.textboxPortAWSNeptune.text())
          sshHandle = self.parent.database.AWSNeptuneConnect(sshAddress, sshUsername, sshPkey, endpoint, port)
          self.parent.isConnect = True
          self.parent.dbCon = "aws neptune"
          self.parent.sshHandle = sshHandle
          self.parent.sshEnabled = True
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setReadOnly(False)
          self.tabQuery.textboxQuery.setPlainText("")
          self.tabQuery.textboxQuery.setFocus()
        except:
          self.parent.sshEnabled = True
          self.parent.sshHandle = None
          self.parent.isConnect = False
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setPlainText("Error conecting to the database")
          self.tabQuery.textboxQuery.setReadOnly(True)
          self.tabQuery.textboxQuery.setFocus()

      if self.comboBox.currentText().lower() == "hbase":
        try:
          host = self.selectDatabase.textboxHostHbase.text()
          port = int(self.selectDatabase.textboxPortHbase.text())
          self.parent.database.HbaseConnect(host, port)
          self.parent.isConnect = True
          self.parent.dbCon = "hbase"
          self.parent.tabQuery.textboxQuery.setReadOnly(True)
          self.tabQuery = hbaseW(self)
        except:
          self.parent.isConnect = False
          self.tabResponse = tabResponseError("")
          self.tabQuery = tabQuery(self)
          self.parent.tab.removeTab(1)
          self.parent.tab.removeTab(0)
          self.parent.tab.addTab(self.tabQuery, 'Query')
          self.parent.tab.addTab(self.tabResponse, 'Response')
          self.tabQuery.textboxQuery.setPlainText("Error conecting to the database")
          self.tabQuery.textboxQuery.setReadOnly(True)
          self.tabQuery.textboxQuery.setFocus()

# Class that shows the graphical interface of the selected databases to complete the connection data
class showOptions(QtWidgets.QDialog):
  def __init__(self, parent=None):
    super(showOptions, self).__init__(parent)

    self.parent = parent
    self.selectComboBox = self.parent.comboBox.currentText()
    self.setStyleSheet("background-color: white;")

    self.font = QtGui.QFont("Arial", 9, QtGui.QFont.Bold)

    if self.selectComboBox.lower() == "mysql":
      self.opt1()

    if self.selectComboBox.lower() == "sqlserver":
      self.opt2()

    if self.selectComboBox.lower() == "oracle":
      self.opt3()

    if self.selectComboBox.lower() == "elasticsearch":
      self.opt4()

    if self.selectComboBox.lower() == "db2":
      self.opt5()

    if self.selectComboBox.lower() == "aws neptune":
      self.opt6()

    if self.selectComboBox.lower() == "hbase":
      self.opt7()

  def opt1(self):
    self.setFixedSize(300, 230)
    self.setWindowTitle(self.selectComboBox)

    QBtn = QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel

    buttonBox = QtWidgets.QDialogButtonBox(QBtn)
    buttonBox.accepted.connect(self.accept)
    buttonBox.rejected.connect(self.reject)
    
    opt1 = QtWidgets.QHBoxLayout()
    labelHost = QtWidgets.QLabel("          Host: ")
    labelHost.setFont(self.font)
    self.textboxHostMysql = QtWidgets.QLineEdit()
    self.textboxHostMysql.setFixedWidth(205)
    opt1.addWidget(labelHost)
    opt1.addWidget(self.textboxHostMysql)

    opt2 = QtWidgets.QHBoxLayout()
    labelPort = QtWidgets.QLabel("           Port: ")
    labelPort.setFont(self.font)
    self.textboxPortMysql = QtWidgets.QLineEdit()
    self.textboxPortMysql.setFixedWidth(205)
    opt2.addWidget(labelPort)
    opt2.addWidget(self.textboxPortMysql)

    opt3 = QtWidgets.QHBoxLayout()
    labelUser = QtWidgets.QLabel("          User: ")
    labelUser.setFont(self.font)
    self.textboxUserMysql = QtWidgets.QLineEdit()
    self.textboxUserMysql.setFixedWidth(205)
    opt3.addWidget(labelUser)
    opt3.addWidget(self.textboxUserMysql)

    opt4 = QtWidgets.QHBoxLayout()
    labelPassw = QtWidgets.QLabel("Password: ")
    labelPassw.setFont(self.font)
    self.textboxPasswMysql = QtWidgets.QLineEdit()
    self.textboxPasswMysql.setFixedWidth(205)
    self.textboxPasswMysql.setEchoMode(QtWidgets.QLineEdit.Password)
    opt4.addWidget(labelPassw)
    opt4.addWidget(self.textboxPasswMysql)

    opt5 = QtWidgets.QHBoxLayout()
    labelDB = QtWidgets.QLabel("  Database: ")
    labelDB.setFont(self.font)
    self.textboxDBMysql = QtWidgets.QLineEdit()
    self.textboxDBMysql.setFixedWidth(205)
    opt5.addWidget(labelDB)
    opt5.addWidget(self.textboxDBMysql)

    layout2 = QtWidgets.QVBoxLayout()
    layout2.addLayout(opt1)
    layout2.addLayout(opt2)
    layout2.addLayout(opt3)
    layout2.addLayout(opt4)
    layout2.addLayout(opt5)

    self.grid = QtWidgets.QGridLayout()
    self.grid.addLayout(layout2, 1, 0, alignment=QtCore.Qt.AlignTop)
    self.grid.addWidget(buttonBox, 2, 0, alignment=QtCore.Qt.AlignBottom)
    self.setLayout(self.grid)

  def opt2(self):
    self.setFixedSize(300, 230)
    self.setWindowTitle(self.selectComboBox)

    QBtn = QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel

    buttonBox = QtWidgets.QDialogButtonBox(QBtn)
    buttonBox.accepted.connect(self.accept)
    buttonBox.rejected.connect(self.reject)

    opt1 = QtWidgets.QHBoxLayout()
    labelHost = QtWidgets.QLabel("       Server: ")
    labelHost.setFont(self.font)
    self.textboxServerSqlServer = QtWidgets.QLineEdit("localhost")
    self.textboxServerSqlServer.setFixedWidth(205)
    opt1.addWidget(labelHost)
    opt1.addWidget(self.textboxServerSqlServer)

    opt2 = QtWidgets.QHBoxLayout()
    labelPort = QtWidgets.QLabel("           Port: ")
    labelPort.setFont(self.font)
    self.textboxPortSqlServer = QtWidgets.QLineEdit("1433")
    self.textboxPortSqlServer.setFixedWidth(205)
    opt2.addWidget(labelPort)
    opt2.addWidget(self.textboxPortSqlServer)

    opt3 = QtWidgets.QHBoxLayout()
    labelUser = QtWidgets.QLabel("          User: ")
    labelUser.setFont(self.font)
    self.textboxUserSqlServer = QtWidgets.QLineEdit("cti23547")
    self.textboxUserSqlServer.setFixedWidth(205)
    opt3.addWidget(labelUser)
    opt3.addWidget(self.textboxUserSqlServer)

    opt4 = QtWidgets.QHBoxLayout()
    labelPassw = QtWidgets.QLabel("Password: ")
    labelPassw.setFont(self.font)
    self.textboxPasswSqlServer = QtWidgets.QLineEdit("Enero2021")
    self.textboxPasswSqlServer.setFixedWidth(205)
    self.textboxPasswSqlServer.setEchoMode(QtWidgets.QLineEdit.Password)
    opt4.addWidget(labelPassw)
    opt4.addWidget(self.textboxPasswSqlServer)

    opt5 = QtWidgets.QHBoxLayout()
    labelDB = QtWidgets.QLabel("  Database: ")
    labelDB.setFont(self.font)
    self.textboxDBSqlServer = QtWidgets.QLineEdit("prueba")
    self.textboxDBSqlServer.setFixedWidth(205)
    opt5.addWidget(labelDB)
    opt5.addWidget(self.textboxDBSqlServer)

    layout2 = QtWidgets.QVBoxLayout()
    layout2.addLayout(opt1)
    layout2.addLayout(opt2)
    layout2.addLayout(opt3)
    layout2.addLayout(opt4)
    layout2.addLayout(opt5)

    self.grid = QtWidgets.QGridLayout()
    self.grid.addLayout(layout2, 1, 0, alignment=QtCore.Qt.AlignTop)
    self.grid.addWidget(buttonBox, 2, 0, alignment=QtCore.Qt.AlignBottom)
    self.setLayout(self.grid)

  def opt3(self):
    self.setFixedSize(300, 230)
    self.setWindowTitle(self.selectComboBox)
    self.instantClient = None

    QBtn = QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel

    buttonBox = QtWidgets.QDialogButtonBox(QBtn)
    buttonBox.accepted.connect(self.accept)
    buttonBox.rejected.connect(self.reject)

    opt1 = QtWidgets.QHBoxLayout()
    labelHost = QtWidgets.QLabel("               Host: ")
    labelHost.setFont(self.font)
    self.textboxHostOracle = QtWidgets.QLineEdit()
    self.textboxHostOracle.setFixedWidth(190)
    opt1.addWidget(labelHost)
    opt1.addWidget(self.textboxHostOracle)

    opt2 = QtWidgets.QHBoxLayout()
    labelPort = QtWidgets.QLabel("                Port: ")
    labelPort.setFont(self.font)
    self.textboxPortOracle = QtWidgets.QLineEdit()
    self.textboxPortOracle.setFixedWidth(190)
    opt2.addWidget(labelPort)
    opt2.addWidget(self.textboxPortOracle)

    opt3 = QtWidgets.QHBoxLayout()
    labelUser = QtWidgets.QLabel("               User: ")
    labelUser.setFont(self.font)
    self.textboxUserOracle = QtWidgets.QLineEdit()
    self.textboxUserOracle.setFixedWidth(190)
    opt3.addWidget(labelUser)
    opt3.addWidget(self.textboxUserOracle)

    opt4 = QtWidgets.QHBoxLayout()
    labelPassw = QtWidgets.QLabel("      Password: ")
    labelPassw.setFont(self.font)
    self.textboxPasswOracle = QtWidgets.QLineEdit()
    self.textboxPasswOracle.setFixedWidth(190)
    self.textboxPasswOracle.setEchoMode(QtWidgets.QLineEdit.Password)
    opt4.addWidget(labelPassw)
    opt4.addWidget(self.textboxPasswOracle)

    opt5 = QtWidgets.QHBoxLayout()
    labelDB = QtWidgets.QLabel("Service Name: ")
    labelDB.setFont(self.font)
    self.textboxSNOracle = QtWidgets.QLineEdit()
    self.textboxSNOracle.setFixedWidth(190)
    opt5.addWidget(labelDB)
    opt5.addWidget(self.textboxSNOracle)

    layout2 = QtWidgets.QVBoxLayout()
    layout2.addLayout(opt1)
    layout2.addLayout(opt2)
    layout2.addLayout(opt3)
    layout2.addLayout(opt4)
    layout2.addLayout(opt5)

    self.grid = QtWidgets.QGridLayout()
    self.grid.addLayout(layout2, 1, 0, alignment=QtCore.Qt.AlignTop)
    self.grid.addWidget(buttonBox, 2, 0, alignment=QtCore.Qt.AlignBottom)
    self.setLayout(self.grid)

  def opt4(self):
    self.setFixedSize(300, 130)
    self.setWindowTitle(self.selectComboBox)

    QBtn = QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel

    buttonBox = QtWidgets.QDialogButtonBox(QBtn)
    buttonBox.accepted.connect(self.accept)
    buttonBox.rejected.connect(self.reject)
    
    opt1 = QtWidgets.QHBoxLayout()
    labelHost = QtWidgets.QLabel("        Host: ")
    labelHost.setFont(self.font)
    self.textboxHostElasticSearch = QtWidgets.QLineEdit()
    self.textboxHostElasticSearch.setFixedWidth(210)
    opt1.addWidget(labelHost)
    opt1.addWidget(self.textboxHostElasticSearch)

    opt2 = QtWidgets.QHBoxLayout()
    labelPort = QtWidgets.QLabel("         Port: ")
    labelPort.setFont(self.font)
    self.textboxPortElasticSearch = QtWidgets.QLineEdit()
    self.textboxPortElasticSearch.setFixedWidth(210)
    opt2.addWidget(labelPort)
    opt2.addWidget(self.textboxPortElasticSearch)

    layout2 = QtWidgets.QVBoxLayout()
    layout2.addLayout(opt1)
    layout2.addLayout(opt2)

    self.grid = QtWidgets.QGridLayout()
    self.grid.addLayout(layout2, 1, 0, alignment=QtCore.Qt.AlignTop)
    self.grid.addWidget(buttonBox, 2, 0, alignment=QtCore.Qt.AlignBottom)
    self.setLayout(self.grid)

  def opt5(self):
    self.setFixedSize(300, 230)
    self.setWindowTitle(self.selectComboBox)

    QBtn = QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel

    buttonBox = QtWidgets.QDialogButtonBox(QBtn)
    buttonBox.accepted.connect(self.accept)
    buttonBox.rejected.connect(self.reject)

    opt1 = QtWidgets.QHBoxLayout()
    labelHost = QtWidgets.QLabel("        Host: ")
    labelHost.setFont(self.font)
    self.textboxHostDB2 = QtWidgets.QLineEdit()
    self.textboxHostDB2.setFixedWidth(210)
    opt1.addWidget(labelHost)
    opt1.addWidget(self.textboxHostDB2)

    opt2 = QtWidgets.QHBoxLayout()
    labelPort = QtWidgets.QLabel("         Port: ")
    labelPort.setFont(self.font)
    self.textboxPortDB2 = QtWidgets.QLineEdit()
    self.textboxPortDB2.setFixedWidth(210)
    opt2.addWidget(labelPort)
    opt2.addWidget(self.textboxPortDB2)

    opt3 = QtWidgets.QHBoxLayout()
    labelUser = QtWidgets.QLabel("        User: ")
    labelUser.setFont(self.font)
    self.textboxUserDB2 = QtWidgets.QLineEdit()
    self.textboxUserDB2.setFixedWidth(210)
    opt3.addWidget(labelUser)
    opt3.addWidget(self.textboxUserDB2)

    opt4 = QtWidgets.QHBoxLayout()
    labelPassw = QtWidgets.QLabel("Password: ")
    labelPassw.setFont(self.font)
    self.textboxPasswDB2 = QtWidgets.QLineEdit()
    self.textboxPasswDB2.setFixedWidth(210)
    self.textboxPasswDB2.setEchoMode(QtWidgets.QLineEdit.Password)
    opt4.addWidget(labelPassw)
    opt4.addWidget(self.textboxPasswDB2)

    opt5 = QtWidgets.QHBoxLayout()
    labelDB = QtWidgets.QLabel("Database: ")
    labelDB.setFont(self.font)
    self.textboxDBDB2 = QtWidgets.QLineEdit()
    self.textboxDBDB2.setFixedWidth(210)
    opt5.addWidget(labelDB)
    opt5.addWidget(self.textboxDBDB2)

    layout2 = QtWidgets.QVBoxLayout()
    layout2.addLayout(opt1)
    layout2.addLayout(opt2)
    layout2.addLayout(opt3)
    layout2.addLayout(opt4)
    layout2.addLayout(opt5)

    self.grid = QtWidgets.QGridLayout()
    self.grid.addLayout(layout2, 1, 0, alignment=QtCore.Qt.AlignTop)
    self.grid.addWidget(buttonBox, 2, 0, alignment=QtCore.Qt.AlignBottom)
    self.setLayout(self.grid)

  def opt6(self):
    self.setFixedSize(300, 230)
    self.setWindowTitle(self.selectComboBox)

    QBtn = QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel

    buttonBox = QtWidgets.QDialogButtonBox(QBtn)
    buttonBox.accepted.connect(self.accept)
    buttonBox.rejected.connect(self.reject)

    opt1 = QtWidgets.QHBoxLayout()
    labelSshUser = QtWidgets.QLabel("      SSH User: ")
    labelSshUser.setFont(self.font)
    self.textboxSshUserAWSNeptune = QtWidgets.QLineEdit()
    self.textboxSshUserAWSNeptune.setFixedWidth(190)
    opt1.addWidget(labelSshUser)
    opt1.addWidget(self.textboxSshUserAWSNeptune)

    opt2 = QtWidgets.QHBoxLayout()
    labelSshAddress = QtWidgets.QLabel("SSH Address: ")
    labelSshAddress.setFont(self.font)
    self.textboxSshAddressAWSNeptune = QtWidgets.QLineEdit()
    self.textboxSshAddressAWSNeptune.setFixedWidth(190)
    opt2.addWidget(labelSshAddress)
    opt2.addWidget(self.textboxSshAddressAWSNeptune)

    opt3 = QtWidgets.QHBoxLayout()
    labelSshPkey = QtWidgets.QLabel("     SSH Pkey: ")
    labelSshPkey.setFont(self.font)
    self.textboxSshPkeyAWSNeptune = QtWidgets.QLineEdit()
    self.textboxSshPkeyAWSNeptune.setFixedWidth(190)
    opt3.addWidget(labelSshPkey)
    opt3.addWidget(self.textboxSshPkeyAWSNeptune)

    opt4 = QtWidgets.QHBoxLayout()
    labelEndpoint = QtWidgets.QLabel("      Endpoint: ")
    labelEndpoint.setFont(self.font)
    self.textboxEndpointAWSNeptune = QtWidgets.QLineEdit()
    self.textboxEndpointAWSNeptune.setFixedWidth(190)
    opt4.addWidget(labelEndpoint)
    opt4.addWidget(self.textboxEndpointAWSNeptune)

    opt5 = QtWidgets.QHBoxLayout()
    labelPort = QtWidgets.QLabel("               Port: ")
    labelPort.setFont(self.font)
    self.textboxPortAWSNeptune = QtWidgets.QLineEdit()
    self.textboxPortAWSNeptune.setFixedWidth(190)
    opt5.addWidget(labelPort)
    opt5.addWidget(self.textboxPortAWSNeptune)

    layout2 = QtWidgets.QVBoxLayout()
    layout2.addLayout(opt1)
    layout2.addLayout(opt2)
    layout2.addLayout(opt3)
    layout2.addLayout(opt4)
    layout2.addLayout(opt5)

    self.grid = QtWidgets.QGridLayout()
    self.grid.addLayout(layout2, 1, 0, alignment=QtCore.Qt.AlignTop)
    self.grid.addWidget(buttonBox, 2, 0, alignment=QtCore.Qt.AlignBottom)
    self.setLayout(self.grid)

  def opt7(self):
    self.setFixedSize(300, 130)
    self.setWindowTitle(self.selectComboBox)

    QBtn = QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel

    buttonBox = QtWidgets.QDialogButtonBox(QBtn)
    buttonBox.accepted.connect(self.accept)
    buttonBox.rejected.connect(self.reject)
    
    opt1 = QtWidgets.QHBoxLayout()
    labelHost = QtWidgets.QLabel("        Host: ")
    labelHost.setFont(self.font)
    self.textboxHostHbase = QtWidgets.QLineEdit()
    self.textboxHostHbase.setFixedWidth(210)
    opt1.addWidget(labelHost)
    opt1.addWidget(self.textboxHostHbase)

    opt2 = QtWidgets.QHBoxLayout()
    labelPort = QtWidgets.QLabel("         Port: ")
    labelPort.setFont(self.font)
    self.textboxPortHbase = QtWidgets.QLineEdit()
    self.textboxPortHbase.setFixedWidth(210)
    opt2.addWidget(labelPort)
    opt2.addWidget(self.textboxPortHbase)

    layout2 = QtWidgets.QVBoxLayout()
    layout2.addLayout(opt1)
    layout2.addLayout(opt2)

    self.grid = QtWidgets.QGridLayout()
    self.grid.addLayout(layout2, 1, 0, alignment=QtCore.Qt.AlignTop)
    self.grid.addWidget(buttonBox, 2, 0, alignment=QtCore.Qt.AlignBottom)
    self.setLayout(self.grid)  

# Generate the Query tab
class tabDatabaseResponse(QtWidgets.QWidget):
  def __init__(self, cols, rows, parent=None):
    super(tabDatabaseResponse, self).__init__(parent)
    self.cols = cols
    self.rows = rows

    self.sizeWidth = 830
    self.sizeHeight = 379

    self.table = QtWidgets.QTableWidget()
    self.make_table()

  def make_table(self):
    self.table.setColumnCount(len(self.cols))
    self.table.setRowCount(len(self.rows))

    font = QtGui.QFont("Arial", 12)

    maxColumnWidth = 0
    maxRowHeight = 0

    self.table.setHorizontalHeaderLabels(self.cols)
    for i in range(len(self.cols)):
      hitem = self.table.horizontalHeaderItem(i)
      self.table.horizontalHeaderItem(i).setFont(font)
      self.table.horizontalHeaderItem(i).setTextAlignment(QtCore.Qt.AlignLeft)
      hitem.setBackground(QtGui.QBrush(QtCore.Qt.yellow))

    for i in range(len(self.rows)):
      for j in range(len(self.rows[i])):
        item = QtWidgets.QTableWidgetItem(str(self.rows[i][j]))
        item.setFont(font)
        self.table.setItem(i, j, item)

    self.table.resizeColumnsToContents()
    self.table.resizeRowsToContents()

    self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

    for c in range(len(self.cols)):
      maxColumnWidth = max(maxColumnWidth, self.table.columnWidth(c))

    rowHeight = 19 * (len(self.rows) + 1)

    maxRowHeight = max(maxRowHeight, rowHeight)

    if maxColumnWidth > self.sizeWidth:
      self.table.setFixedWidth(maxColumnWidth)
    else:
      self.table.setFixedWidth(self.sizeWidth)

    if maxRowHeight > self.sizeHeight:
      self.table.setFixedHeight(maxRowHeight)
    else:
      self.table.setFixedHeight(self.sizeHeight)

    self.hbox12 = QtWidgets.QGridLayout()
    self.hbox12.addWidget(self.table, 0, 0)
    self.setLayout(self.hbox12)

# Class that handles Hbase connection and queries
class hbaseW(QtWidgets.QWidget):
  def __init__(self, parent=None):
    super(hbaseW, self).__init__(parent)

    self.parent = parent
    self.tab = self.parent.parent.tab
    self.database = self.parent.parent.database

    self.tab.removeTab(1)
    self.tab.removeTab(0)

    self.tabResponse = tabResponseError("")
    self.tabHbF = tabHbaseFirst(0, [], self)

    self.tab.addTab(self.tabHbF, 'Query')
    self.tab.addTab(self.tabResponse, 'Response')

# Generate the first tab
class tabHbaseFirst(QtWidgets.QWidget):
  def __init__(self, cols, rows, parent=None):
    super(tabHbaseFirst, self).__init__(parent)

    self.parent = parent
    self.tab = self.parent.tab
    self.database = self.parent.database
    self.tabResponse =self.parent.tabResponse

    self.tUpd = tabHbaseUPdate(cols, rows, self)

# Update the tab for Hbase
class tabHbaseUPdate(QtWidgets.QWidget):
  def __init__(self, cols, rows, parent=None):
    super(tabHbaseUPdate, self).__init__(parent)

    self.setFixedWidth(850)
    self.setFixedHeight(400)

    self.parent = parent
    self.tab = self.parent.tab
    self.database = self.parent.database
    self.tabResponse =self.parent.tabResponse

    font = QtGui.QFont("Arial", 12)

    self.cb = comboBoxHbase()
    self.table = tableHbase(cols, rows)
  
    executeButton = QtWidgets.QPushButton("Execute")
    executeButton.setFixedHeight(30)
    executeButton.setFixedWidth(80)
    executeButton.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
    executeButton.setStyleSheet("background-color: #4bce69; color: white; margin-right: 12px;")
    executeButton.setFont(font)

    self.hbox = QtWidgets.QVBoxLayout()
    self.hbox.addWidget(self.cb)
    self.hbox.addWidget(self.table)
    self.hbox.addWidget(executeButton, alignment=QtCore.Qt.AlignRight)
    self.setLayout(self.hbox)

    self.cb.comboBox.activated.connect(self.showItems)
    executeButton.clicked.connect(self.runQuery)

  # Executes selected Hbase actions
  def runQuery(self):
    if self.cb.comboBox.currentIndex() == 1:
      response = self.database.HbaseQuery(1)
      self.tabResponse = tabResponseError(response)
      self.tab.removeTab(1)
      self.tab.addTab(self.tabResponse, 'Response')

    if self.cb.comboBox.currentIndex() == 2:
      query = []
      col = {}

      name = self.table.table.cellWidget(0, 1).text()
      cols = self.table.table.cellWidget(1, 1).text().split(",")

      for c in cols:
        col[c.strip()] = dict()

      query.append(name)
      query.append(col)
      response = self.database.HbaseQuery(2, query)
      self.tabResponse = tabResponseError(response)
      self.tab.removeTab(1)
      self.tab.addTab(self.tabResponse, "Response")

    if self.cb.comboBox.currentIndex() in [3, 4, 5, 6, 7, 8, 9, 13]:
      name = self.table.table.cellWidget(0, 1).text()
      response = self.database.HbaseQuery(self.cb.comboBox.currentIndex(), name)
      self.tabResponse = tabResponseError(response)
      self.tab.removeTab(1)
      self.tab.addTab(self.tabResponse, "Response")

    if self.cb.comboBox.currentIndex() in [10, 14, 15]:
      query = []
      col = []

      name = self.table.table.cellWidget(0, 1).text()
      row = self.table.table.cellWidget(1, 1).text()
      cols = self.table.table.cellWidget(2, 1).text().split(",")

      for c in cols:
        col.append(c.strip())

      query.append(name)
      query.append(row)
      query.append(col)
      response = self.database.HbaseQuery(self.cb.comboBox.currentIndex(), query)
      self.tabResponse = tabResponseError(response)
      self.tab.removeTab(1)
      self.tab.addTab(self.tabResponse, "Response")

    if self.cb.comboBox.currentIndex() == 11:
      query = []
      row = []
      col = []

      name = self.table.table.cellWidget(0, 1).text()
      rows = self.table.table.cellWidget(1, 1).text().split(",")
      cols = self.table.table.cellWidget(2, 1).text().split(",")

      for c in cols:
        col.append(c.strip())

      for r in rows:
        row.append(r.strip())

      query.append(name)
      query.append(row)
      query.append(col)

      response = self.database.HbaseQuery(11, query)
      self.tabResponse = tabResponseError(response)
      self.tab.removeTab(1)
      self.tab.addTab(self.tabResponse, "Response")

    if self.cb.comboBox.currentIndex() == 12:
      query = []

      name = self.table.table.cellWidget(0, 1).text()
      row = self.table.table.cellWidget(1, 1).text()
      col = self.table.table.cellWidget(2, 1).text()

      query.append(name)
      query.append(row)
      query.append(col)
      response = self.database.HbaseQuery(12, query)
      self.tabResponse = tabResponseError(response)
      self.tab.removeTab(1)
      self.tab.addTab(self.tabResponse, "Response")

    if self.cb.comboBox.currentIndex() == 14:
      value = []
      query = []

      name = self.table.table.cellWidget(0, 1).text()
      row = self.table.table.cellWidget(1, 1).text()
      col = self.table.table.cellWidget(2, 1).text()
      value = self.table.table.cellWidget(3, 1).text()

      query.append(name)
      query.append(row)
      query.append(col)
      query.append(value)

      response = self.database.HbaseQuery(14, query)
      self.tabResponse = tabResponseError(response)
      self.tab.removeTab(1)
      self.tab.addTab(self.tabResponse, "Response")

    if self.cb.comboBox.currentIndex() == 16:
      query = []

      name = self.table.table.cellWidget(0, 1).text()
      row = self.table.table.cellWidget(1, 1).text()

      query.append(name)
      query.append(row)

      response = self.database.HbaseQuery(16, query)
      self.tabResponse = tabResponseError(response)
      self.tab.removeTab(1)
      self.tab.addTab(self.tabResponse, "Response")

  # Show a dropdown list of actions for Hbase
  def showItems(self):
    if self.cb.comboBox.currentIndex() == 1:
      self.grid1()

    if self.cb.comboBox.currentIndex() == 2:
      self.grid2()

    if self.cb.comboBox.currentIndex() in [3, 4, 5, 6, 7, 8, 9]:
      self.grid3()

    if self.cb.comboBox.currentIndex() == 10:
      self.grid4()

    if self.cb.comboBox.currentIndex() == 11:
      self.grid5()

    if self.cb.comboBox.currentIndex() == 12:
      self.grid6()

    if self.cb.comboBox.currentIndex() == 13:
      self.grid3()

    if self.cb.comboBox.currentIndex() == 14:
      self.grid7()

    if self.cb.comboBox.currentIndex() == 15:
      self.grid4()

    if self.cb.comboBox.currentIndex() == 16:
      self.grid8()

  def grid1(self):
    self.tabQuery = tabHbaseFirst(0, [], self)
    self.tabResponse = tabResponseError("")
    self.tab.removeTab(1)
    self.tab.removeTab(0)
    self.tab.addTab(self.tabQuery, "Query")
    self.tab.addTab(self.tabResponse, "Response")
    self.tabQuery.tUpd.cb.comboBox.setCurrentIndex(1)

  def grid2(self):
    self.tabQuery = tabHbaseFirst(2, [("Name:", "Table name"), ("Families:", "Enter values separated by commas")], self)
    self.tabResponse = tabResponseError("")
    self.tab.removeTab(1)
    self.tab.removeTab(0)
    self.tab.addTab(self.tabQuery, "Query")
    self.tab.addTab(self.tabResponse, "Response")
    self.tabQuery.tUpd.cb.comboBox.setCurrentIndex(2)

  def grid3(self):
    self.tabQuery = tabHbaseFirst(2, [("Name:", "Table name")], self)
    self.tab.removeTab(1)
    self.tab.removeTab(0)
    self.tab.addTab(self.tabQuery, "Query")
    self.tab.addTab(self.tabResponse, "Response")
    self.tabQuery.tUpd.cb.comboBox.setCurrentIndex(self.cb.comboBox.currentIndex())

  def grid4(self):
    self.tabQuery = tabHbaseFirst(2, [("Name:", "Table name"), ("Row Key:", "Row Key"), ("Column:", "Enter values separated by commas")], self)
    self.tab.removeTab(1)
    self.tab.removeTab(0)
    self.tab.addTab(self.tabQuery, "Query")
    self.tab.addTab(self.tabResponse, "Response")
    self.tabQuery.tUpd.cb.comboBox.setCurrentIndex(self.cb.comboBox.currentIndex())

  def grid5(self):
    self.tabQuery = tabHbaseFirst(2, [("Name:", "Table name"), ("Row Key:", "Row Key (Enter values separated by commas"), ("Column:", "Colum Name (Enter values separated by commas)")], self)
    self.tab.removeTab(1)
    self.tab.removeTab(0)
    self.tab.addTab(self.tabQuery, "Query")
    self.tab.addTab(self.tabResponse, "Response")
    self.tabQuery.tUpd.cb.comboBox.setCurrentIndex(self.cb.comboBox.currentIndex())

  def grid6(self):
    self.tabQuery = tabHbaseFirst(2, [("Name:", "Table name"), ("Row Key:", "Row Key"), ("Column:", "Column Name")], self)
    self.tab.removeTab(1)
    self.tab.removeTab(0)
    self.tab.addTab(self.tabQuery, "Query")
    self.tab.addTab(self.tabResponse, "Response")
    self.tabQuery.tUpd.cb.comboBox.setCurrentIndex(self.cb.comboBox.currentIndex())

  def grid7(self):
    self.tabQuery = tabHbaseFirst(2, [("Name:", "Table name"), ("Row Key:", "Row Key"), ("Column:", "Column name must include a family and qualifier part (may be the empty string) => \"cf:col\" or \"cf:\""), ("value:", "Value")], self)
    self.tab.removeTab(1)
    self.tab.removeTab(0)
    self.tab.addTab(self.tabQuery, "Query")
    self.tab.addTab(self.tabResponse, "Response")
    self.tabQuery.tUpd.cb.comboBox.setCurrentIndex(self.cb.comboBox.currentIndex())

  def grid8(self):
    self.tabQuery = tabHbaseFirst(2, [("Name:", "Table name"), ("Row Key:", "Row Key")], self)
    self.tab.removeTab(1)
    self.tab.removeTab(0)
    self.tab.addTab(self.tabQuery, "Query")
    self.tab.addTab(self.tabResponse, "Response")
    self.tabQuery.tUpd.cb.comboBox.setCurrentIndex(self.cb.comboBox.currentIndex())

# dropdown list
class comboBoxHbase(QtWidgets.QWidget):
  def __init__(self, parent=None):
    super(comboBoxHbase, self).__init__(parent)

    self.comboBox = QtWidgets.QComboBox()

    self.comboBox.addItems(
      [
        "Choose an option",
        "Show a list of table name", 
        "Create a table",
        "Delete the specified table",
        "Enable the specified table",
        "Disable the specified table",
        "Show whether the specified table is enabled",
        "Compact the specified table",
        "Retrieve the column families for this table",
        "Retrieve the regions for this table",
        "Retrieve a single row of data",
        "Retrieve multiple rows of data",
        "Retrieve multiple versions of a single cell from the table",
        "Create a scanner for data in the table",
        "Store data in the table",
        "Delete data from the table specifying a column by row key",
        "Delete data from the table of all columns by row key",
      ]
    )

    hbox = QtWidgets.QHBoxLayout()
    hbox.addWidget(self.comboBox)

    self.setLayout(hbox)

# Complete the graphical interface of the Query tab
class tableHbase(QtWidgets.QWidget):
  def __init__(self, cols, rows, parent=None):
    super(tableHbase, self).__init__(parent)

    self.cols = cols
    self.rows = rows

    self.table = QtWidgets.QTableWidget()
    self.table.horizontalHeader().hide()
    self.table.verticalHeader().hide()
    self.table.setShowGrid(False)
    self.table.setStyleSheet("margin: 3px; padding: 5px;")

    self.make_table()

  def make_table(self):
    self.table.setColumnCount(self.cols)
    self.table.setRowCount(len(self.rows))

    font = QtGui.QFont("Arial", 12, QtGui.QFont.Bold)

    for i in range(len(self.rows)):
      item = QtWidgets.QTableWidgetItem(str(self.rows[i][0]))
      item.setFont(font)
      item.setTextAlignment(QtCore.Qt.AlignCenter | QtCore.Qt.AlignRight)
      self.table.setItem(i, 0, item)

      text = QtWidgets.QLineEdit()
      text.setFixedWidth(700)
      text.setPlaceholderText(self.rows[i][1])
      self.table.setCellWidget(i, 1, text)

    self.table.resizeColumnsToContents()
    self.table.resizeRowsToContents()

    self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

    self.hbox = QtWidgets.QHBoxLayout()
    self.hbox.addWidget(self.table)
    self.setLayout(self.hbox)

# Class that handles the connection and query of databases
class databases():

  def mysqlConnect(self, host, port, user, passw, database):
    self.con = mysql.connector.connect(
      host = host,
      port = port,
      user = user,
      password = passw,
      database = database
    )

  def mysqlQuery(self, q):
    try:
      cur = self.con.cursor()
      cur.execute(q)
      resList = cur.fetchall()

      if cur.rowcount > 0:
        columns = cur.column_names
        return columns, resList
      else:
        return None, None
    except:
      return "Error", "Error"

  def sqlServerConnect(self, server, port, user, passw, database):
    self.con = pyodbc.connect(
      'DRIVER={SQL Server};' +
      'SERVER=' + server +
      ';PORT=' + port +
      ';DATABASE=' + database +
      ';UID=' + user +
      ';PWD=' + passw + 
      ';Trusted_Connection=yes;'
    )

  def sqlServerQuery(self, q):
    try:
      cur = self.con.cursor()
      cur.execute(q)

      try:
        resList = cur.fetchall()
      except:
        resList = []
        cur.commit()
        pass

      if len(resList) > 0:
        column = []
        columns = cur.description
        for columnTemp in columns:
          column.append(columnTemp[0])
        return column, resList
      else:
        return None, None
    except:
      return "Error", "Error"

  def db2Connect(self, host, port, user, passw, database):
    self.con = db.connect(
      "DATABASE=" + database + 
      ";HOSTNAME=" + host +
      ";PORT=" + port +
      ";PROTOCOL=TCPIP" +
      ";UID=" + user +
      ";PWD=" + passw + ";", 
      "",
      ""
    )

  def db2Query(self, q):
    try:
      cur = self.con.cursor()
      cur.execute(q)
      resList = cur.fetchall()

      if len(resList) > 0:
        column = []
        columns = cur.description
        for columnTemp in columns:
          column.append(columnTemp[0])
        return column, resList
      else:
        return None, None
    except:
      return "Error", "Error"

  def oracleConnect(self, localhost, user, passw, port, serviceName):
    self.con = cx_Oracle.connect(user+"/"+passw+"@"+localhost+":"+port+"/"+serviceName)

  def oracleQuery(self, q):
    try:
      cur = self.con.cursor()
      self.con.autocommit = True
      cur.execute(q)
      resList = cur.fetchall()

      if len(resList) > 0:
        column = []
        columns = cur.description
        for columnTemp in columns:
          column.append(columnTemp[0])
        return column, resList
      else:
        return None, None
    except:
      return "Error", "Error"

  def elasticConnect(self, host, port):
    if not host.startswith("http"):
      host = "http://" + host + ":" + port
    else:
      host += ":" + port

    if host[-1] == "/":
      host = host[:-1]

    self.host = host

    source = requests.get(self.host)

    json.loads(source.text)

  def elasticQuery(self, query):
    try:
      method = ""
      data = ""
      url = ""
      fd = False

      if query.strip().lower().startswith("get "):
        method = str(query[:4]).strip().upper()
        url = str(query[4:]).strip()

      if query.strip().lower().startswith("post "):
        method = str(query[:5]).strip().upper()
        url = str(query[5:]).strip()

      if query.strip().lower().startswith("put "):
        method = str(query[:4]).strip().upper()
        url = str(query[4:]).strip()

      if query.lower().startswith("delete "):
        method = str(query[:7]).strip().upper()
        url = str(query[7:]).strip()

      data = re.search("\n({[\s\S]+})", url)
      if data:
        fd = True

        url = url.replace(data[1], "").strip()
        data = json.loads(data[1].strip())

      if url[-1] == "/":
        url = url[:-1]

      url = self.host + "/" + url

      if method == "GET":
        if fd:
          s = requests.post(url, json = data)
        else:
          s = requests.get(url)

      if method == "POST":
        s = requests.post(url, json = data)

      if method == "PUT":
        s = requests.put(url, json = data)

      if method == "DELETE":
        if fd:
          s = requests.post(url, json = data)
        else:
          s = requests.delete(url)

      pretty_json = json.loads(s.text)

      return(json.dumps(pretty_json, indent=4))

    except:
      return "Error"

  def AWSNeptuneConnect(self, sshAddress, sshUsername, sshPkey, endpoint, port):
    self.server = SSHTunnelForwarder(
      ssh_address_or_host=(sshAddress, 22),
      ssh_username = sshUsername,
      ssh_pkey = sshPkey,
      remote_bind_address=(endpoint, port),
      local_bind_address=(endpoint, port)
    )
    self.server.start()
    self.url = "https://" + endpoint + ":" + str(port)
    requests.get(self.url)

    return self.server

  def AWSNeptuneQuery(self, query):
    try:
      source = requests.post(self.url + "/gremlin", json = {"gremlin": query})
      pretty_json = json.loads(source.text)
      return json.dumps(pretty_json, indent=4)
    except:
      return "Error"

  def HbaseConnect(self, host, port):
    self.con = happybase.Connection(host = host, port = port)
    self.con.open()

  def HbaseQuery(self, index, query = {}):
    if index == 1:
      resp = ""
      for table in self.con.tables():
        resp += table.decode("utf-8") + "\n"
      
      return resp

    if index == 2:
      try:
        table = query[0]
        cols = query[1]
        self.con.create_table(table, cols)
        return "Table created ok"
      except Exception as e:
        return str(e)

    if index == 3:
      try:
        table = query
        self.con.delete_table(table)
        return "Deleted table ok"
      except Exception as e:
        try:
          if "TableNotDisabledException" in str(e):
            return "Table Not Disabled"
        except:
          return "Query Error"
      return "Query Error"

    if index == 4:
      try:
        table = query
        self.con.enable_table(table)
        return "Table Enabled"
      except Exception as e:
        try:
          if "TableNotDisabledException" in str(e):
            return "Table Enabled"
        except:
          return "Query Error"
      return "Query Error"

    if index == 5:
      try:
        table = query
        self.con.disable_table(table)
        return "Table Disabled"
      except Exception as e:
        try:
          if "TableNotEnabledException" in str(e):
            return "Table Disabled"
        except:
          return "Query Error"
      return "Query Error"

    if index == 6:
      try:
        table = query
        if self.con.is_table_enabled(table):
          return "Table Enabled"
        else:
          return "Table Disabled"
      except:
        return "Query Error"

    if index == 7:
      try:
        table = query
        self.con.compact_table(table)
        return "Compact table ok"
      except:
        return "Query Error"

    if index == 8:
      try:
        resp = ""
        table = query
        conT = happybase.Table(table, self.con)
        families = list(conT.families().values())

        for f in range(len(families)):
          ff = str(families[f])
          ff = ff.replace(" b'", "")
          ff = ff.replace("'}", "}")
          ff = ff.replace("{'", "{")
          ff = ff.replace(":',", "',")
          ff = ff.replace("', '", ", ")
          ff = ff.replace("', ", ", ")
          ff = ff.replace(", '", ", ")
          ff = ff.replace("':", " => ")
          resp += ff

          if f < len(families)-1:
            resp += "\n\n"

        return resp

      except:
        return "Query Error"

    if index == 9:
      try:
        resp = ""
        table = query
        conT = happybase.Table(table, self.con)
        regions = conT.regions()

        for r in range(len(regions)):
          resp += str(regions[r])

          if r < len(regions)-1:
            resp += "\n\n"

        return resp

      except:
        return "Query Error"

    if index == 10:
      try:
        resp = ""
        cols = []
        cell = []
        column = []
        maxLong = 0
        table = query[0]
        row = str(query[1])
        conT = happybase.Table(table, self.con)

        for col in query[2]:
          cols.append(col.encode("utf-8"))

        rows = conT.row(row, cols)
        keys = list(rows.keys())
        values = list(rows.values())

        for i in range(len(keys)):
          column.append(keys[i].decode("utf-8"))
          cell.append(values[i].decode("utf-8"))
          maxLong = max(maxLong, len(keys[i].decode("utf-8")))

        spaces = maxLong + 12

        spacesTemp = spaces - len("COLUMN")
        resp += "COLUMN" + str(" "*spacesTemp) + "CELL\n"
        for i in range(len(column)):
          spacesTemp = spaces - len(column[i])
          resp += column[i] + str(" "*spacesTemp) + cell[i]

          if i < len(rows)-1:
            resp += "\n"

        return resp

      except:
        return "Query Error"

    if index == 11:
      try:
        resp = ""
        cols = []
        keys = []
        values = []
        rowsKeys = []
        maxLong1 = 0
        maxLong2 = 0
        table = query[0]
        rowsC = query[1]

        conT = happybase.Table("prueba", self.con)

        for col in query[2]:
          cols.append(col.encode("utf-8"))

        rows = conT.rows(rowsC, cols)

        for x, y in rows:
          for i in list(y.keys()):
            rowsKeys.append(x.decode("utf-8"))
            maxLong2 = max(maxLong2, len(x.decode("utf-8")))
            keys.append(i.decode("utf-8"))
            maxLong1 = max(maxLong1, len(i.decode("utf-8")))
          for i in list(y.values()):
            values.append(i.decode("utf-8"))

        spaces = maxLong1 + 12
        spacesRows = maxLong2 + 8

        spacesTemp = spaces - len("COLUMN")
        spacesTemp2 = spacesRows - len("ROW")
        resp += "ROW" + str(" "*spacesTemp2) + "COLUMN" + str(" "*spacesTemp) + "CELL\n"
        for i in range(len(keys)):
          spacesTemp = spaces - len(keys[i])
          spacesTemp2 = spacesRows - len(rowsKeys[i])
          resp += rowsKeys[i] + str(" "*spacesTemp2) + keys[i] + str(" "*spacesTemp) + values[i]

          if i < len(keys)-1:
            resp += "\n"

        return resp

      except:
        return "Query Error"

    if index == 12:
      try:
        resp = ""
        table = query[0]
        row = query[1]
        col = query[2]

        conT = happybase.Table(table, self.con)
        versions = conT.cells(row, col)

        for i in range(len(versions)):
          resp += versions[i].decode("utf-8")

          if i < len(versions)-1:
            resp += "\n"

        return resp

      except:
        return "Query Error"

    if index == 13:
      try:
        resp = ""
        keys = []
        values = []
        rowsKeys = []
        maxLong1 = 0
        maxLong2 = 0
        table = query

        conT = happybase.Table(table, self.con)
        scan = conT.scan()

        for x, y in scan:
          for i in list(y.keys()):
            rowsKeys.append(x.decode("utf-8"))
            maxLong2 = max(maxLong2, len(x.decode("utf-8")))
            keys.append(i.decode("utf-8"))
            maxLong1 = max(maxLong1, len(i.decode("utf-8")))
          for i in list(y.values()):
            values.append(i.decode("utf-8"))

        spaces = maxLong1 + 12
        spacesRows = maxLong2 + 8

        spacesTemp = spaces - len("COLUMN")
        spacesTemp2 = spacesRows - len("ROW")
        resp += "ROW" + str(" "*spacesTemp2) + "COLUMN" + str(" "*spacesTemp) + "CELL\n"
        for i in range(len(keys)):
          spacesTemp = spaces - len(keys[i])
          spacesTemp2 = spacesRows - len(rowsKeys[i])
          resp += rowsKeys[i] + str(" "*spacesTemp2) + keys[i] + str(" "*spacesTemp) + values[i]

          if i < len(keys)-1:
            resp += "\n"

        return resp

      except:
        return "Query Error"

    if index == 14:
      try:
        table = query[0]
        row = query[1]
        col = query[2]
        value = query[3]

        conT = happybase.Table(table, self.con)

        row = conT.put(row, {col.strip(): value.strip()})

        return "Stored data ok"

      except:
        return "Query Error"

    if index == 15:
      try:
        table = query[0]
        row = query[1]
        cols = query[2]
        columns = []

        for col in cols:
          columns.append((col))

        conT = happybase.Table(table, self.con)
        conT.delete(row, columns)

        return "Delete data ok"

      except:
        return "Query Error"

    if index == 16:
      try:
        table = query[0]
        row = query[1]

        conT = happybase.Table(table, self.con)
        conT.delete(row)

        return "Delete data ok"

      except:
        return "Query Error"

# run the program
if __name__ == '__main__':
  app = QtWidgets.QApplication(sys.argv)
  app.setStyle(QtWidgets.QStyleFactory.create('Fusion'))
  w = Main()
  w.show()
  sys.exit(app.exec_())
