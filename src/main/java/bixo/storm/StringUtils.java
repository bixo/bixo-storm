package bixo.storm;

public class StringUtils {

    /**
     * Generate a 64-bit JOAAT hash from the bytes of <s>
     * 
     * @param s String to hash
     * @return 64-bit hash
     */
    public static long getLongHash(String s) {
        long result = 0;

        char[] chars = s.toCharArray();
        
        for (char curChar : chars) {
            int h = (int)curChar;
            
            result += h & 0x0FFL;
            result += (result << 20);
            result ^= (result >> 12);
            
            h = h >> 8;
        
            result += h & 0x0FFL;
            result += (result << 20);
            result ^= (result >> 12);
        }
        
        result += (result << 6);
        result ^= (result >> 22);
        result += (result << 30);

        return result;
    }

}
