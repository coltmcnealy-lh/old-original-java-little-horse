/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package little.horse;

import java.util.Date;
import java.util.Locale;

public class App {
    public static void main(String[] args) {
        System.out.println("TODO: run some experiment for funsies.");
        System.out.println("This class does nothing, all the logic is elsewhere.");

        long foo = 1;
        System.out.println(String.valueOf(foo));
        System.out.println(
            String.format(Locale.US, "%020d", new Date().getTime())
        );
    }
}
