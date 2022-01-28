package Project.login;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import static Project.Constants.LOGIN_CREDENTIALS_FILE;

public class LoginUser  {

    public boolean loginUser() throws IOException {

        List<String> securityQuestionList = Arrays.asList("Which is your " +
                "favorite superhero?","Which is your dream city to live in?",
            "What was your favorite subject in school?");

        System.out.println("Enter credential for user");
        Scanner sc=new Scanner(System.in);
        System.out.println("Enter email-id:");
        String username=sc.nextLine();
        System.out.println("Enter password:");
        String password=sc.nextLine();


        Random rand = new Random();
        String randomElement = securityQuestionList.get(rand.nextInt(securityQuestionList.size()));
        System.out.println(randomElement);
        int questionIndex=securityQuestionList.indexOf(randomElement);
        String securityAnswer=sc.nextLine();

        final String passwordSha256Hash = HashAlgorithmUtil.getSHA256Hash(password);
        password = passwordSha256Hash;

        final String securityAnswerHash = HashAlgorithmUtil.getSHA256Hash(securityAnswer);
        securityAnswer=securityAnswerHash;


        FileReader fr = new FileReader(LOGIN_CREDENTIALS_FILE);
        BufferedReader br = new BufferedReader(fr); //Creation of BufferedReader object
        String s;
        String[] words = null;
        while ((s = br.readLine()) != null)   //Reading Content from the file
        {
            words = s.split(" "); //Split the word using space
            if (words[0].equals(username) && words[1].equals(password) && (words[(questionIndex + 2)].equalsIgnoreCase(securityAnswer))) {
                return true;
            }
        }
        return false;
    }
}