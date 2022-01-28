package Project.login;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import static Project.Constants.LOGIN_CREDENTIALS_FILE;


public class RegisterUser {
    public boolean register() throws IOException {
        List<String> securityQuestionList = Arrays.asList("Which is your " +
                "favorite superhero?","Which is your dream city to live in?",
            "What was your favorite subject in school?");

        System.out.println("Enter credential for user");
        Scanner sc=new Scanner(System.in);
        System.out.println("Enter email-id:");
        String username=sc.nextLine();
        System.out.println("Enter password:");
        String password=sc.nextLine();

        ArrayList<String> securityAnswer=new ArrayList<>();
            for (int i = 0; i < securityQuestionList.size(); i++) {
                System.out.println(securityQuestionList.get(i));
                securityAnswer.add(sc.nextLine());
            }

        // user object null
        if (username == null || password==null) {
            System.out.println("username or password is null Please re-Enter user details");
            return false;
        }else if(securityAnswer.get(0)==null ||securityAnswer.get(1)==null ||securityAnswer.get(2)==null ){
            System.out.println("one security question is null Please re-Enter" +
                " security answers");
            return false;
        }

        FileReader fr = new FileReader(LOGIN_CREDENTIALS_FILE);
        BufferedReader br = new BufferedReader(fr); //Creation of BufferedReader object
        String s;
        String[] words=null;
        while((s=br.readLine())!=null)   //Reading Content from the file
        {
            words=s.split(" ");  //Split the word using space
            for (String word : words)
            {
                if (word.equals(username))   //Search for the given word
                {
                    System.out.println("user already registered");
                    return false;
                }
            }
        }
        if (!Pattern.matches("^[a-zA-Z0-9_+&*-]+(?:\\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,}$",username)){
            System.out.println("username is not valid\nPlease re-Enter user details");
            return false;
        }

        final String passwordSha256Hash = HashAlgorithmUtil.getSHA256Hash(password);
        password=passwordSha256Hash;

        for(int j=0;j<securityQuestionList.size();j++) {
            final String securityAnswerHash =
                HashAlgorithmUtil.getSHA256Hash(securityAnswer.get(j));
            securityAnswer.set(j, securityAnswerHash);
        }

        FileWriter writer = new FileWriter(LOGIN_CREDENTIALS_FILE, true);

        writer.write(username + " "+ password+" "+securityAnswer.get(0)+" "+securityAnswer.get(1)+" "+securityAnswer.get(2)+"\n");
        writer.close();
        return true;

    }
}
