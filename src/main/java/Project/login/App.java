package Project.login;

import java.io.IOException;
import java.util.Arrays;

public class App {
  public static void main(String[] args) throws IOException {
    RegisterUser registerUser=new RegisterUser();
    if(registerUser.register()){
      System.out.println("registered successfully");
    }
    LoginUser loginUser=new LoginUser();
    if(loginUser.loginUser()){
      System.out.println("User Logged in");

    }
    else{
      System.out.println("User credential is not valid");
    }
  }
}
