import java.sql.*;

public class Main {

    public static void main(String[] args) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:adls:AuthScheme=AzureAD;Schema=ADLSGen2;Account=storagev2heirarchical;FileSystem=blobcontainerheirarchical;AzureTenant=94be69e7-edb4-4fda-ab12-95bfc22b232f;InitiateOAuth=GETANDREFRESH;ConnectOnOpen='True';Logfile=D:\\My logs\\lake.txt;Verbosity=3;SSLServerCert=*;ProxyServer=localhost;ProxyPort=8888;");
        Statement stat = conn.createStatement();
//        boolean ret = stat.execute("DELETE FROM ProductImages WHERE ProductId = 'gid://shopify/Product/1418253271063' AND Id = 'gid://shopify/ProductImage/30682180714519'\n");
//        boolean ret = stat.execute("UPDATE \"GRAPHQL-2023-07\".\"ProductImages\" SET AltText = 'This is the main image' WHERE 'Id' = 'gid://shopify/ProductImage/30139032535063' and \"ProductId\" = 'gid://shopify/Product/1384062648343';\n");
//        boolean ret = stat.execute("select * from sys_tables");
//        if (ret) {
//            ResultSet rs=stat.getResultSet();
//            while(rs.next()) {
//                for(int i=1;i<=rs.getMetaData().getColumnCount();i++) {
//                    System.out.println(rs.getMetaData().getColumnLabel(i) +"="+rs.getObject(i));
//                }

                CallableStatement cstmt = conn.prepareCall("UploadFile");
                cstmt.setString("Path", "Account/University.txt");
                cstmt.setString("FilePath", "D:\\Program Files\\CData\\CData JDBC Driver for Azure Data Lake Storage 2023\\Test\\University.txt");

        boolean ret = cstmt.execute();
                if (!ret) {
                    int count=cstmt.getUpdateCount();
                    if (count!=-1) {
                        System.out.println("Affected rows: "+count);
                    }
                }
                else {
                    ResultSet rs=cstmt.getResultSet();
                    while(rs.next()){
                        for(int i=1;i<=rs.getMetaData().getColumnCount();i++) {
                            System.out.println(rs.getMetaData().getColumnLabel(i) +"="+rs.getString(i));
                        }
                    }
                }
            }
        }
