import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by ania on 9/3/2019.
 */
public class FileSortTest {


    @Test
    public void test_moreMemorySpaceThanFile()throws IOException{
        FileSort.tempFilesList = new ArrayList();
        FileSort.main(new String[] {"-x","100","-f","0"});
        Assert.assertEquals(FileSort.tempFilesList.size(),1);
    }


    @Test
    public void test_SortedTempFilesCreated()throws IOException{
        FileSort.tempFilesList = new ArrayList();
        FileSort.main(new String[] {"-x","10","-f","0"});
        Assert.assertEquals(FileSort.tempFilesList.size(),2);
    }



    @Test(expectedExceptions = IllegalArgumentException.class)
    public void test_negativeMemory_assertThrowsException()throws IOException{
        FileSort.main(new String[] {"-x","-1","-f","0"});
    }
}
