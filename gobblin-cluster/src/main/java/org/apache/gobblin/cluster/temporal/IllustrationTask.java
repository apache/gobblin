package org.apache.gobblin.cluster.temporal;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/** Generally, this would specify what "work" needs performing plus how to perform; now just a unique name (to log) */
@Data
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class IllustrationTask {
    @NonNull
    private String name;
}
