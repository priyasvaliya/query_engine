package Project;

import static Project.Constants.LOCAL_PATH;
import java.io.File;
import java.io.IOException;

public class DataBase {

  public String currentDatabase;
  public boolean create(String name) throws IOException {
    boolean status = false;
    File databaseFolder = new File(LOCAL_PATH+name);
    if(!databaseFolder.exists()){
      status = databaseFolder.mkdirs();
    }
    File databaseLock = new File(LOCAL_PATH + name + "/lock.txt");
    databaseLock.createNewFile();
    return status;
  }

  public void use(String name){
    currentDatabase = name;
  }
}
