package uk.co.devworx.xmlflattener;

import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeReflection;

public class GraalVMFeatureSetup implements Feature
{
    public void beforeAnalysis(BeforeAnalysisAccess access)
    {
        try
        {
            RuntimeReflection.register(java.util.LinkedHashMap.class.getDeclaredConstructor());
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

}
