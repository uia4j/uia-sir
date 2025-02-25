package uia.sir;

public class HandleEx {

    public static String SITE = "1020";

    public static String trim(String handle) {
        if (handle == null || handle.trim().length() == 0) {
            return null;
        }

        int index = handle.indexOf(":" + SITE + ",");
        if (index < 0) {
            return handle;
        }

        String name = handle.substring(index + SITE.length() + 2);
        // operation: ,#
        index = name.indexOf(",");
        if (index >= 0) {
            name = name.substring(0, Math.min(handle.length(), index));
        }
        return name;
    }
}
