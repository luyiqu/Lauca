package input;

import java.io.*;

public class AnonymityInfoSerializer {

    public void write(Anonymity anonymity, File output) {
        try (ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(output))) {
            writer.writeObject(anonymity);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    public Anonymity read(File input) {
        try (ObjectInputStream reader = new ObjectInputStream(new FileInputStream(input))) {
            return (Anonymity) reader.readObject();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
