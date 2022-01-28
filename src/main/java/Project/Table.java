package Project;

import java.io.*;
import java.util.*;

import static Project.Constants.LOCAL_PATH;

public class Table {

  /**
   * This method is to get consolidated table.
   * Eg: (key, value)--> (C1, [V1 V2])
   *
   * @param br
   * @return
   * @throws IOException
   */
  public HashMap<String, ArrayList<String>> getRecords(BufferedReader br) throws IOException {
    HashMap<String, ArrayList<String>> records = new HashMap<>();
    ArrayList<String> temp;
    String st;
    while ((st = br.readLine()) != null) {
      if (st.length() > 0) {
        String[] rec = st.split(" ");
        if (records.containsKey(rec[0])) {
          temp = new ArrayList<>(records.get(rec[0]));
        } else {
          temp = new ArrayList<>();
        }
        temp.add(rec[1]);
        records.put(rec[0], temp);
      }
    }
    return records;
  }

  /**
   * This method is to get values of specific column in a table.
   * Eg: [V1 V2]
   *
   * @param col
   * @param records
   * @return
   */
  public ArrayList<String> getColumn(String col, HashMap<String, ArrayList<String>> records) {
    ArrayList<String> temp = null;
    for (Map.Entry<String, ArrayList<String>> ee : records.entrySet()) {
      String key = ee.getKey();
      if (col.equals(key)) {
        temp = ee.getValue();
      }
    }
    return temp;
  }

  public String getPrimaryKeyColumn(String dbName, String tableName) throws IOException {
    File dataDict = new File(LOCAL_PATH + dbName + "/dataDictionary.txt");
    BufferedReader dataDictReader = new BufferedReader(new FileReader(dataDict));
    String st;
    String nl;
    while ((st = dataDictReader.readLine()) != null) {
      if (st.length() > 0) {
        if (st.equals(tableName)) {
          while ((nl = dataDictReader.readLine()) != null) {
            String[] tmp = nl.split(" ");
            if (tmp.length == 3) {
              return tmp[0];
            }
          }
        }
      }
    }

    return null;
  }

  public ArrayList<String> getLockedFile(String dbName) throws IOException {
    File file = new File(LOCAL_PATH + dbName + "/lock.txt");
    FileWriter lockWriter = new FileWriter(file, true);
    BufferedReader content = new BufferedReader(new FileReader(file));
    ArrayList<String> lFiles = new ArrayList<>();
    String st;
    while ((st = content.readLine()) != null) {
      lFiles.add(st);
    }
    return lFiles;
  }

  public void setLockFile(String dbName, String tableName, boolean condition) throws IOException {
    File file = new File(LOCAL_PATH + dbName + "/lock.txt");
    FileWriter lockWriter = new FileWriter(file);
    ArrayList<String> lFiles = getLockedFile(dbName);
    if (condition) {
      lFiles.add(tableName);
    } else {
      lFiles.remove(tableName);
    }
    StringBuilder list = new StringBuilder();
    for (String name : lFiles) {
      list.append(name);
    }
    System.out.println(list.toString());
    lockWriter.write(list.toString());
    lockWriter.close();
  }


  public boolean create(String tableName, String userName, String databaseName, ArrayList<String> columns, ArrayList<String> valuesTypes, HashMap<String, String> keySet) {
    // Creating Data dict. inside the database folder
    try {
      File dataDict = new File(LOCAL_PATH + databaseName + "/dataDictionary.txt");

      // Let's check first if this file exist in database or not
      if (dataDict.createNewFile()) {
        System.out.println("Created new Data dictionary for database " + databaseName);
      }

      FileWriter dataDictWriter = new FileWriter(dataDict, true);


      // Assumption that this table does not exist
      dataDictWriter.append(tableName);
      dataDictWriter.append("\n");

      // Appending the columns and values seperated by space
      for (int i = 0; i < columns.size(); i++) {
        dataDictWriter.append(columns.get(i));
        dataDictWriter.append(" ");
        dataDictWriter.append(valuesTypes.get(i));
        if (keySet.containsKey(columns.get(i))) {
          dataDictWriter.append(" ");
          dataDictWriter.append(keySet.get(columns.get(i)));
        }
        dataDictWriter.append("\n");
      }
      dataDictWriter.append("\n");
      dataDictWriter.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
    // Creating a file with same name under the database
    try {
      File tableFile = new File(LOCAL_PATH + databaseName + "/" + tableName + ".txt");
      tableFile.createNewFile();
      return true;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }

  }

  public boolean insert(String tableName, String userName, String databaseName, ArrayList<String> columns, ArrayList<String> values) throws IOException {
    // Assuming user will send columnNames along with the query in correct order
    // If column Names are not present send back the error
    // Assuming that user will send all columns
    // Assuming table exist

    //TODO: Check for primary key

    String pk = getPrimaryKeyColumn(databaseName, tableName);


    if (columns != null) {
      ArrayList<String> colValues = new ArrayList<>();
      File tableFile = new File(LOCAL_PATH + databaseName + "/" + tableName + ".txt");
      if (pk == null) {
        System.out.println("No Primary key in database");
      } else {
        BufferedReader bf = new BufferedReader(new FileReader(tableFile));
        colValues = getColumn(pk, getRecords(bf));
      }
      FileWriter tableFileWriter = new FileWriter(tableFile, true);
      if (!tableFile.exists()) {
        System.out.println("Table Doesn't exist");
        return false;
      }
      for (int i = 0; i < columns.size(); i++) {
        if(columns.get(i).equals(pk)){
          if (colValues.contains(values.get(i))){
            System.out.println("Duplicate value present in PK");
            return false;
          }
        }
        tableFileWriter.append(columns.get(i));
        tableFileWriter.append(" ");
        tableFileWriter.append(values.get(i));
        tableFileWriter.append("\n");
      }
      tableFileWriter.append("\n");
      tableFileWriter.close();

      return true;
    }

    return false;
  }

  /**
   * This method displays the selected columns orr all(*) values in a table.
   * It can handle simple WHERE clause like WHERE col1=value
   *
   * @param tableName
   * @param databaseName
   * @param columns
   * @return
   * @throws IOException
   */
  public boolean select(String tableName, String databaseName,
                        ArrayList<String> columns,
                        String key, String condition, String value) throws IOException {
    File tableFile = new File(LOCAL_PATH + databaseName + "/" + tableName + ".txt");
    if (!tableFile.exists()) {
      System.out.println("Table Doesn't exist");
      return false;
    }
    BufferedReader br = new BufferedReader(new FileReader(tableFile));
    HashMap<String, ArrayList<String>> records = getRecords(br);
    ArrayList<String> temp;
    ArrayList<Integer> presentIn = new ArrayList<>();
    if (key != null) {
      ArrayList<String> col = records.get(key);
      for (int i = 0; i < col.size(); i++) {
        if (col.get(i).equals(value)) {
          presentIn.add(i);
        }
      }
      System.out.println(presentIn.toString());
    }
    for (Map.Entry<String, ArrayList<String>> ee : records.entrySet()) {
      String columnName = ee.getKey();
      if (columns.contains(columnName) || columns.contains("*")) {
        System.out.print(columnName + "\t");
        temp = ee.getValue();
        for (int i = 0; i < temp.size(); i++) {
          if (key != null && presentIn.contains(i))
            System.out.print(temp.get(i) + "\t\t");
          else if (key == null) System.out.print(temp.get(i) + "\t\t");
        }
        System.out.println("\n");
      }
    }
    return true;
  }

  /**
   * This method displays ER Diagram.
   * The underlined column is the Primary Key. The Bolded column is the Foreign Key.
   *
   * @param tableName
   * @param databaseName
   * @param columns
   * @param values
   * @throws IOException
   */
  public boolean update(String tableName, String databaseName, ArrayList<String> columns,
                        ArrayList<String> values, String condition, String value) throws IOException {
    // Assuming user will send columnNames along with the query in correct order
    // If column Names are not present send back the error
    // Assuming that user will send all columns
    // Assuming table exist

    String conditionColumn = condition;
    String conditionValue = value;
    boolean success = false;
    File fileToBeModified = new File(LOCAL_PATH + databaseName + "/" + tableName + ".txt");
    if (!fileToBeModified.exists()) {
      System.out.println("Table Doesn't exist");
      return false;
    }

    ArrayList<String> lockedFiles = getLockedFile(databaseName);
    if (!lockedFiles.contains(tableName)) {
      setLockFile(databaseName, tableName, true);

      BufferedReader reader = new BufferedReader(new FileReader(fileToBeModified));
      String st;

      HashMap<String, ArrayList<String>> records = new HashMap<>();
      ArrayList<String> temp;
      while ((st = reader.readLine()) != null) {
        if (st.length() > 0) {
          String[] rec = st.split(" ");
          if (records.containsKey(rec[0])) {
            temp = new ArrayList<>(records.get(rec[0]));
          } else {
            temp = new ArrayList<>();
          }
          temp.add(rec[1]);
          records.put(rec[0], temp);
        }
      }

      ArrayList<Integer> presentIn = new ArrayList<>();
      ArrayList<String> col = records.get(conditionColumn);
      for (int i = 0; i < col.size(); i++) {
        if (col.get(i).equals(conditionValue)) {
          presentIn.add(i);
        }
      }

      for (Map.Entry<String, ArrayList<String>> ee : records.entrySet()) {
        temp = ee.getValue();
        for (int i = 0; i < temp.size(); i++) {
          if (columns.contains(ee.getKey()) && presentIn.contains(i)) {
            int index = columns.indexOf(ee.getKey());
            temp.set(i, values.get(index));
            records.put(ee.getKey(), temp);
          }
        }
      }
      FileWriter writer = new FileWriter(fileToBeModified, false);
      for (int i = 0; i < records.get(conditionColumn).size(); i++) {
        for (Map.Entry<String, ArrayList<String>> ee : records.entrySet()) {
          String record = ee.getKey() + " " + ee.getValue().get(i) + "\n";
          writer.write(record);
          writer.flush();
        }
        writer.write("\n");
        writer.flush();
      }
      setLockFile(databaseName, tableName, false);
      System.out.printf("value Updated successfully");
      success = true;
    } else {
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            update(tableName, databaseName, columns, values, condition, value);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }, 500);
    }
    return success;
  }


  /**
   * This method will truncate the table value
   *
   * @param tableName
   * @param databaseName
   * @return
   * @throws IOException
   */
  public boolean truncate(String tableName, String databaseName) throws IOException {
    File tableFile = new File(LOCAL_PATH + "/" + databaseName + "/" + tableName + ".txt");
    if (!tableFile.exists()) {
      System.out.println("Table Doesn't exist");
      return false;
    }

    BufferedReader reader = new BufferedReader(new FileReader(tableFile));
    String st;
    HashMap<String, ArrayList<String>> records = new HashMap<>();
    ArrayList<String> temp = null;
    while ((st = reader.readLine()) != null) {
      if (st.length() > 0) {
        String[] rec = st.split(" ");
        if (records.containsKey(rec[0])) {
          temp = new ArrayList<>(records.get(rec[0]));
        } else {
          temp = new ArrayList<>();
        }
        temp.add(rec[1]);
        records.put(rec[0], temp);
      }
    }
    FileWriter writer = new FileWriter(tableFile, false);
    System.out.println("Table Truncated successfully");
    return true;
  }

  /**
   * This Method will delete the table file and records of data in data
   * dictionary file
   *
   * @param tableName
   * @param databaseName
   * @return
   * @throws IOException
   */
  public boolean dropTable(String tableName, String databaseName) throws IOException {
    File tableFile = new File(LOCAL_PATH + "/" + databaseName + "/" + tableName + ".txt");
    if (!tableFile.exists()) {
      System.out.println("Table Doesn't exist");
      return false;
    } else {
      tableFile.delete();
    }

    File dataDictionaryFile =
        new File(LOCAL_PATH + "/" + databaseName + "/" + "testDataDictionary" +
            ".txt");
    BufferedReader reader = new BufferedReader(new FileReader(dataDictionaryFile));
    String st, tableKey = null;
    HashMap<String, ArrayList<String>> records = new HashMap<>();
    ArrayList<String> temp = null;
    while ((st = reader.readLine()) != null) {
      if (st.length() > 0) {
        String[] array = st.trim().split(" ");
        if (array.length == 1) {
          tableKey = st;
        }
        if (records.containsKey(tableKey)) {
          temp = new ArrayList<>(records.get(tableKey));
        } else {
          temp = new ArrayList<>();
        }
        temp.add(st);
      }
      records.put(tableKey, temp);
    }
    FileWriter writer = new FileWriter(dataDictionaryFile, false);
    for (Map.Entry<String, ArrayList<String>> ee : records.entrySet()) {
      String record = "";
      if (!ee.getKey().equals(tableName)) {
        temp = ee.getValue();
        for (int i = 0; i < temp.size(); i++) {
          record += temp.get(i) + "\n";
        }
        writer.write(record);
        writer.flush();
        writer.write("\n");
        writer.flush();
      }
    }
    System.out.println("table Dropped successfully");
    return true;
  }

  /**
   * This method displays ER Diagram.
   * The underlined column is the Primary Key. The Bolded column is the Foreign Key.
   *
   * @param databaseName
   * @throws IOException
   */
  public boolean erd(String databaseName) throws IOException {
    HashMap<String, ArrayList<String>> list = new HashMap<String, ArrayList<String>>();

    File dataDict = new File(LOCAL_PATH + databaseName + "/dataDictionary.txt");

    if (!dataDict.exists()) {
      System.out.println("Database doesn't exists");
      return false;
    }
    BufferedReader br = new BufferedReader(new FileReader(dataDict));
    String st;
    ArrayList<String> data = new ArrayList<String>();
    int count = 0;
    String table = null;

    while ((st = br.readLine()) != null) {
      if (st.length() > 0) {
        if (count == 0) {
          table = st;
          count = count + 1;
        } else {
          String[] details = st.split("\s");
          boolean isPrimary = details.length == 3;
          boolean isForeign = details.length == 6;
          if (isPrimary) {
            data.add("\033[4m" + details[0] +
                "\033[0m" + " " + details[1]);
          } else if (isForeign) {
            data.add("\033[0;1m" + details[0] + "\033[0;0m" + " " + details[1] + " ref-->" + details[4] );
          } else {
            data.add(details[0] + " " + details[1]);
          }
        }
      } else {
        list.put(table, data);
        count = 0;
        data = new ArrayList<String>();
      }
    }
    System.out.println("\n" +
        "**************************************************************************************************************");
    System.out.println("*                                ER DIAGRAM                                                  " +
        "*");
    System.out.println(
        "**************************************************************************************************************");
    System.out.println("Table" + "\t\t\t" + "| " + "Columns");
    System.out.println(
        "---------------------------------------------------------------------------------------------------------------");
    for (String key : list.keySet()) {
      System.out.print(key + "\t\t\t" + "| ");
      for (String c : list.get(key)) {
        System.out.print(c + "\t\t" + "| ");
      }
      System.out.println("\n" +
          "-------------------------------------------------------------------------------------------------------------");
    }
    return true;
  }

  public boolean dumps(String databaseName) throws IOException {
    String path = LOCAL_PATH + databaseName;
    File folder = new File(path); //read folder
    if (!folder.exists()) {
      System.out.println("Database doesn't exists");
      return false;
    }
    String st;
    StringBuilder built = new StringBuilder();
    BufferedReader br;
    File[] files = folder.listFiles();
    for (File file : files) {
      if (file.getAbsolutePath().indexOf("dataDictionary.tx") >= 0) {
        br = new BufferedReader(new FileReader(file.getAbsolutePath()));
        String create = "";
        int count = 0;
        while ((st = br.readLine()) != null) {
          if (st.length() > 0) {
            if (count == 0) {
              create = create + "CREATE TABLE " + st + " (";
              count = 1;
            } else {
              create = create + st + ",";
            }
          } else {
            count = 0;
            create = create.substring(0, create.length() - 1);
            create = create + ");";
            built.append(create).append("\n");
            create = "";
          }
        }
      } else if (file.getAbsolutePath().indexOf("lock.txt") < 0) {
        String tablePath = file.getAbsolutePath().replace("\\", ";");
        String[] pathSplits = tablePath.split(";");
        String tableName = pathSplits[pathSplits.length - 1];
        tableName = tableName.substring(0, tableName.indexOf("."));
        br = new BufferedReader(new FileReader(file.getAbsolutePath()));
        String columns = "";
        String values = "";
        while ((st = br.readLine()) != null) {
          if (st.length() > 0) {
            int index = st.indexOf("\s");
            columns = columns + st.split("\\s+")[0] + ",";
            values = values + st.substring(index + 1) + ",";
          } else {
            columns = columns.substring(0, columns.length() - 1);
            values = values.substring(0, values.length() - 1);
            built.append("INSERT INTO ").append(tableName).append(" (").append(columns).append(") VALUES (").append(values).append(");\n");
            columns = "";
            values = "";
          }
        }
      }
    }
    File dumpFile = new File(LOCAL_PATH  + "/" + databaseName + "Dumps.txt");
    if (!dumpFile.exists()) {
      dumpFile.createNewFile();
    }
    FileWriter dumpFileWriter = new FileWriter(dumpFile, true);
    dumpFileWriter.write(built.toString());
    dumpFileWriter.close();
    return true;
  }

  public boolean executeDump(String databaseName) throws IOException {
    QueryParser qp = new QueryParser();
    File dumpFile = new File(LOCAL_PATH + databaseName + "Dumps.txt");
    if (!dumpFile.exists()) {
      System.out.println("Database Dump not generated");
      return false;
    }
    BufferedReader br = new BufferedReader(new FileReader(dumpFile));
    String st;
    qp.parseQuery(null,"CREATE DATABASE "+databaseName+";");
    qp.parseQuery(null,"USE "+databaseName+";");
    while ((st = br.readLine()) != null) {
      System.out.println(st);
      qp.parseQuery(databaseName, st);
    }
    return true;
  }
}