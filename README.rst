::
                                                                                            
                                                O                                           
                                               ( ))                                         
                                              (    )                                        
                                             (   )  )                                       
                                            (  )   )                                        
                                             (  ) )                                         
                                              (  )                                          
                                              (   )                                         
                                               ( )                                          
                                      ──┐      ──┐                                          
                                    /   │    /   │                                          
                                   /    │   /    │                                          
                                  /     │  /     │                                          
                                 /      │ /      │                                          
                                /       │/       │                                          
                               /        │        │                                          
                              ┌─────────┴────────┴┐                                         
                              │                   │                                         
                              │ ┌──┐             ┌┘ ┌──┐                                    
                              │ │  │      ┌─┐    │  │  │  ┌──┐                              
                              │ └──┘      │ │    └┬─┴──┴──┴──┴───                           
                              │           │ │     │* * * * * * * O                          
                              └───────────┴─┴─────┘---------------                          
            ____  _            ___               ______           __                        
           / __ \(_)___  ___  / (_)___  ___     / ____/___ ______/ /_____  _______  __      
          / /_/ / / __ \/ _ \/ / / __ \/ _ \   / /_  / __ `/ ___/ __/ __ \/ ___/ / / /      
         / ____/ / /_/ /  __/ / / / / /  __/  / __/ / /_/ / /__/ /_/ /_/ / /  / /_/ /       
        /_/   /_/ .___/\___/_/_/_/ /_/\___/  /_/    \__,_/\___/\__/\____/_/   \__, /        
               /_/                                                           /____/         
                                                                                            

Pipeline Factory is a Python server framework to define, manage, schedule and execute custom defined code pipelines and series of pipeline across multiple worker processes.

- Based on Fastapi and Python multiprocessing
- Define pipelines of action steps to process data, make api calls, generate artifacts etc.
- Resumable pipelines where steps can define recovery action to avoid redoing work
- Run series of pipelines based on state/ results of previous pipelines
- Cron schedule tasks to auto run or resume pipelines
- Tasks that run pipelines execute on a worker process pool
- Add python callables as custom tasks, to extend pipeline execution capabilities or support execution of arbitrary work on worker pool

Installation
============
::

 pip install pipeline-factory

Example usage
=============
`Example app <https://github.com/izo0x90/pipeline-factory/tree/main/example_app>`_

