package Project.frontend;
import Project.QueryParser;
import Project.Table;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;
public class ShowUserQuery {
  QueryParser qp = new QueryParser();
  StringBuilder tr = new StringBuilder();
  Scanner sc=new Scanner(System.in);
  public void listQuery() throws IOException {
    System.out.println("\nPlease select query number That you want to perform");
    Table table=new Table();
    while (true) {
      System.out.println("\n1. EXECUTE QUERY");
      System.out.println("2. Export ERD database");
      System.out.println("3. Export Dumps database");
      System.out.println("4. Execute Dump Database");
      System.out.println("5. Transaction");
      System.out.println("6. Exit");
      System.out.println("Select an option");
      String dbName;
      final String input = sc.nextLine();
      switch (input) {
        case "1":
          System.out.println("Please enter your Query");
          String query=sc.nextLine();
          qp.parseQuery(null,query);
          break;
        case "2":
          System.out.println("Enter the database name");
          dbName =  sc.nextLine();
          table.erd(dbName);
          break;
        case "3":
          System.out.println("Enter the database name");
          dbName =  sc.nextLine();
          table.dumps(dbName);
          break;
        case "4":
          System.out.println("Enter the database name");
          dbName =  sc.nextLine();
          table.executeDump(dbName);
          break;
        case "5":
          this.queue(qp);
        case "6":
          return;
        default:
          break;
      }
    }
  }

  public void queue (QueryParser qp) throws IOException {
    System.out.println("Please enter your transaction queries");
    String del = sc.nextLine();
    if(!del.equals("commit;")) {
      tr.append(del+"\n");
      this.queue(qp);
    } else {
      String[] q = tr.toString().split("\n");
      System.out.println("Q is "+ Arrays.toString(q));
      for(String c: q) {
        qp.parseQuery(null, c);
      }
    }
  }
}