namespace DataImporter {
    public class Program {

        [STAThread]
        public static void Main() {
            Application.SetCompatibleTextRenderingDefault(false);
            Application.EnableVisualStyles();
            Application.Run(new FileManager());
        }
    }
}