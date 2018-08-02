import com.gurps.avro.Person;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static com.gurps.avro.Gender.MALE;


public class TestAvro {

    static final String FILENAME = "person-test.avro";

    @Test
    void roundtripSerialisation() {
        Person person = Person.newBuilder()
                              .setId(1)
                              .setFirstName("fred")
                              .setLastName("flintstone")
                              .setEmailAddress("fred.flintstone@bedrock.com")
                              .setJoinDate("2017-06-01")
                              .setBirthdate("1982-01-12")
                              .setPhoneNumber("91822")
                              .setMiddleName("bernard")
                              .setSex(MALE)
                              .setSiblings(2)
                              .setUsername("fredflintstone")
                              .build();

        writeToDisk(person);
        readFromDisk();
    }

    private void writeToDisk(Person person) {
        File output = new File(FILENAME);
        try {
            DatumWriter<Person> personDatumWriter = new SpecificDatumWriter<>(Person.class);
            DataFileWriter<Person> dataFileWriter = new DataFileWriter<>(personDatumWriter);
            dataFileWriter.create(person.getSchema(), output);
            dataFileWriter.append(person);
            dataFileWriter.append(person);
            dataFileWriter.close();
        } catch (IOException e) {
            System.out.println("Error writing Avro");
        }
    }

    private void readFromDisk() {
        try {
            DatumReader<Person> personDatumReader = new SpecificDatumReader(Person.class);
            DataFileReader<Person> dataFileReader = new DataFileReader<>(new File(FILENAME), personDatumReader);
            Person person = null;
            while (dataFileReader.hasNext()) {
                person = dataFileReader.next(person);
                System.out.println(person);
            }
        } catch (IOException e) {
            System.out.println("Error reading Avro");
        }
    }
}
