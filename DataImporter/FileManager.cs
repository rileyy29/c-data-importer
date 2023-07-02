using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;

namespace DataImporter {
    public partial class FileManager : Form {

        private OpenFileDialog openFileDialog;
        private TextBox connectionString;
        private TextBox tableName;
        private Button selectButton;
        private ProgressBar progressBar;
        private Label progressLabel;
        private BackgroundWorker backgroundWorker;

        public FileManager() {
            InitializeComponent();
            this.Text = "Data Importer";
            this.Icon = Icon.ExtractAssociatedIcon(Application.ExecutablePath);

            connectionString = new TextBox {
                Size = new Size(190, 20),
                Location = new Point(12, 15),
                PlaceholderText = "Connection String"
            };
            tableName = new TextBox {
                Size = new Size(100, 20),
                Location = new Point(209, 15),
                PlaceholderText = "Table Name"
            };
            openFileDialog = new OpenFileDialog();
            selectButton = new Button {
                Size = new Size(296, 25),
                Location = new Point(12, 40),
                Text = "Select File"
            };
            selectButton.Click += new EventHandler(SelectButton_Click);
            progressBar = new ProgressBar {
                Location = new Point(12, 85),
                Name = "progressBar",
                Size = new Size(296, 23),
                TabIndex = 1
            };
            progressLabel = new Label {
                Location = new Point(11, 70),
                Size = new Size(296, 23),
                Text = ""
            };

            ClientSize = new Size(330, 125);
            Controls.Add(connectionString);
            Controls.Add(tableName);
            Controls.Add(selectButton);
            Controls.Add(progressBar);
            Controls.Add(progressLabel);
        }

        private void SelectButton_Click(object sender, EventArgs e) {
            try {
                if (openFileDialog.ShowDialog() != DialogResult.OK) {
                    return;
                }

                if (String.IsNullOrEmpty(openFileDialog.FileName)) {
                    throw new Exception("Please select a file");
                }

                if (!openFileDialog.CheckFileExists || !openFileDialog.FileName.EndsWith(".csv")) {
                    throw new Exception("The selected file must be a readable .CSV");
                }

                if (String.IsNullOrEmpty(connectionString.Text)) {
                    throw new Exception("Please enter a valid connection string");
                }

                if (String.IsNullOrEmpty(tableName.Text)) {
                    throw new Exception("Please enter a valid database table name");
                }

                progressLabel.Text = "Reading data...";
                ReadFile();
            } catch (Exception ex) {
                MessageBox.Show(ex.Message);
            }

        }

        private void backgroundWorker_ProgressChanged(object sender, ProgressChangedEventArgs e) {
            progressBar.Value = e.ProgressPercentage;
            progressLabel.Text = e.ProgressPercentage + "%";

            if (e.ProgressPercentage == 100) {
                toggleSelectButton(true);
            }
        }

        private void backgroundWorker_DoWork(object sender, DoWorkEventArgs e) {
            DataTable dt = e.Argument as DataTable;
            if (dt == null) {
                return;
            }

            SqlServerBulkUpsert bulkAction = new SqlServerBulkUpsert(new SqlConnection(connectionString.Text), null, tableName.Text,
               Array.Empty<string>(), Array.Empty<string>(), Array.Empty<string>());

            SqlMergeResults result = bulkAction.DoWith(dt.Rows.OfType<DataRow>(), (r, row) => {
                foreach (DataColumn column in dt.Columns) {
                    string columnName = column.ColumnName;
                    row[columnName] = r[columnName];
                }
            });

            for (int i = 0; i <= 100; i++) {
                if (backgroundWorker.CancellationPending) {
                    e.Cancel = true;
                    return;
                }

                backgroundWorker.ReportProgress(i);
            }

            MessageBox.Show($"Successfully inserted {result.RowsInserted} rows, updated {result.RowsUpdated} rows and removed {result.RowsDeleted} rows.");
        }

        private void toggleSelectButton(Boolean enabled) {
            selectButton.Enabled = enabled;
        }

        private void ReadFile() {
            toggleSelectButton(false);

            DataTable dt = buildTableFromCSV(openFileDialog.FileName);

            if (dt == null) {
                MessageBox.Show("No records could be found.");
                progressLabel.Text = "";
                toggleSelectButton(true);
                return;
            }

            progressLabel.Text = "Updating data...";

            backgroundWorker = new BackgroundWorker();
            backgroundWorker.DoWork += new DoWorkEventHandler(backgroundWorker_DoWork);
            backgroundWorker.ProgressChanged += new ProgressChangedEventHandler(backgroundWorker_ProgressChanged);
            backgroundWorker.WorkerReportsProgress = true;

            backgroundWorker.RunWorkerAsync(argument: dt);
            progressBar.Value = 0;
        }

        public static DataTable buildTableFromCSV(string csv_file_path) {
            DataTable csvData = new DataTable();
            try {
                if (csv_file_path.EndsWith(".csv")) {
                    using (Microsoft.VisualBasic.FileIO.TextFieldParser csvReader = new Microsoft.VisualBasic.FileIO.TextFieldParser(csv_file_path)) {
                        csvReader.SetDelimiters(new string[] { "," });
                        csvReader.HasFieldsEnclosedInQuotes = true;
                        
                        string[] colFields = csvReader.ReadFields();
                        foreach (string column in colFields) {
                            DataColumn datecolumn = new DataColumn(column);
                            datecolumn.AllowDBNull = true;
                            csvData.Columns.Add(datecolumn);
                        }
                        while (!csvReader.EndOfData) {
                            string[] fieldData = csvReader.ReadFields();
                            for (int i = 0; i < fieldData.Length; i++) {
                                if (fieldData[i] == "") {
                                    fieldData[i] = null;
                                }
                            }
                            csvData.Rows.Add(fieldData);
                        }
                    }
                }
            } catch (Exception ex) {
                MessageBox.Show("CSV Error: " + ex);
            }
            return csvData;
        }

    }
}
