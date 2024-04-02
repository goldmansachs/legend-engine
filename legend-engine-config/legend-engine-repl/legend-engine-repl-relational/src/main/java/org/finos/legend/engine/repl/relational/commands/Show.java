// Copyright 2024 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.repl.relational.commands;

import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.engine.repl.client.Client;
import org.finos.legend.engine.repl.core.Command;
import org.finos.legend.engine.repl.relational.httpServer.ReplGridServer;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.awt.Desktop;
import java.net.URI;

public class Show implements Command
{
    private Client client;

    public ReplGridServer replGridServer;

    public Show(Client client, ReplGridServer replGridServer)
    {
        this.client = client;
        this.replGridServer = replGridServer;
    }

    @Override
    public String documentation()
    {
        return "show";
    }

    @Override
    public boolean process(String line) throws Exception
    {
        if (line.startsWith("show"))
        {
            if (this.replGridServer.canShowGrid())
            {
                System.out.println("Unable to show repl grid, no query has been executed");
            }
            else
            {
                try
                {
                   if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE))
                   {
                       Desktop.getDesktop().browse(URI.create("http://localhost:8080/repl/grid"));
                   }
                   else
                   {
                       System.out.println("Unable to show repl grid, default web browser is not configured");
                   }
                }
                catch (Exception e)
                {
                    System.out.println(e.getMessage());
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public MutableList<Candidate> complete(String inScope, LineReader lineReader, ParsedLine parsedLine)
    {
        return null;
    }
}
