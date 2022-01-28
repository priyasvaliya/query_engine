package Project.frontend;

import Project.login.LoginUser;
import Project.login.RegisterUser;

import java.io.IOException;
import java.util.Scanner;

public class DataBaseEntry {
  public static void main(String[] args) throws IOException {
    System.out.println("Welcome!!");
    while (true) {
      System.out.println("\n1. Register user");
      System.out.println("2. User Log in");
      System.out.println("3. Exit");
      System.out.println("select an option from above");
      Scanner sc = new Scanner(System.in);
      int input = sc.nextInt();

      switch (input) {
        case 1: {

          RegisterUser registerUser = new RegisterUser();
          if (registerUser.register()) {
            System.out.println("registered successfully");
          } else {
            System.out.println("Please enter valid credential");

          }
          break;
        }
        case 2: {
          LoginUser loginUser = new LoginUser();
          if (loginUser.loginUser()) {
            System.out.printf("User Logged in\n");
            ShowUserQuery showUserQuery=new ShowUserQuery();

            showUserQuery.listQuery();
          } else {
            System.out.printf("User credential is not valid");
          }
          break;
        }
        case 3:
          System.exit(0);
        default:
          break;
      }
    }
  }

}
